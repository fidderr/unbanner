const fs = require('fs');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const AdblockerPlugin = require('puppeteer-extra-plugin-adblocker');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const cheerio = require('cheerio');
const path = require('path');

puppeteer.use(StealthPlugin());
puppeteer.use(AdblockerPlugin({ blockTrackers: true }));

const SUBREDDIT = 'nederlands';
const TARGET_REASON = 'de voertaal is Nederlands';
const BANLIST_URL = `https://www.reddit.com/mod/${SUBREDDIT}/banned?pageSize=100`;
const COOKIES_FILE = 'cookies.json';
const PAGE_LIMIT = 999;
const CONCURRENCY_LIMIT = 20;
const MIN_DELAY = 20;
const MAX_DELAY = 50;

const HTML_DIR = path.join(__dirname, 'scrape_html');
const RESULT_DIR = path.join(__dirname, 'result');
if (!fs.existsSync(HTML_DIR)) fs.mkdirSync(HTML_DIR);
if (!fs.existsSync(RESULT_DIR)) fs.mkdirSync(RESULT_DIR);

['language_ban_review.csv', 'unban_only.csv', 'log.txt'].forEach(file => {
  const fullPath = path.join(RESULT_DIR, file);
  if (fs.existsSync(fullPath)) fs.unlinkSync(fullPath);
});

const log = (...args) => {
  const msg = args.map(a => (typeof a === 'string' ? a : JSON.stringify(a))).join(' ');
  console.log(msg);
  fs.appendFileSync(path.join(RESULT_DIR, 'log.txt'), msg + '\n');
};

// Foreground lock
const frontLock = (() => {
  let isLocked = false;
  const queue = [];
  return {
    acquire: () => new Promise(resolve => {
      if (!isLocked) {
        isLocked = true;
        resolve();
      } else {
        queue.push(resolve);
      }
    }),
    release: () => {
      if (queue.length > 0) {
        const next = queue.shift();
        next();
      } else {
        isLocked = false;
      }
    }
  };
})();

const randomDelay = (base) => {
  const jitter = Math.floor(Math.random() * (MAX_DELAY - MIN_DELAY + 1)) + MIN_DELAY;
  const wait = base + jitter;
  return new Promise(resolve => setTimeout(resolve, wait));
};

async function smartLogin(page) {
  if (fs.existsSync(COOKIES_FILE)) {
    const cookies = JSON.parse(fs.readFileSync(COOKIES_FILE));
    await page.setCookie(...cookies);
    await page.goto('https://www.reddit.com/', { waitUntil: 'domcontentloaded' });

    const isLoggedOut = await page.$('a[href*="/login"]');
    if (!isLoggedOut) {
      log('✅ Reused session via cookies (already logged in).');
      return;
    }

    log('⚠️ Cookies found but not valid. Manual login required.');
  } else {
    log('🔑 No cookies file. Manual login required.');
  }

  await page.goto('https://www.reddit.com/login', { waitUntil: 'domcontentloaded' });

  console.log('\n⏳ Please log in manually in the browser.');
  console.log('👉 Press ENTER in this terminal once you are fully logged in.\n');

  await new Promise(resolve => {
    process.stdin.resume();
    process.stdin.once('data', () => {
      process.stdin.pause();
      resolve();
    });
  });

  const cookies = await page.cookies();
  fs.writeFileSync(COOKIES_FILE, JSON.stringify(cookies, null, 2));
  log('✅ Cookies saved. Continuing...');
}


async function processWithConcurrencyPool(tasks, limit, handler) {
  const results = [];
  const executing = new Set();

  for (const task of tasks) {
    await randomDelay(1);
    const p = handler(task).then(result => {
      log(`🟢 Done: ${task.username}`);
      executing.delete(p);
      return result;
    });
    log(`🟡 Starting: ${task.username}`);
    results.push(p);
    executing.add(p);
    if (executing.size >= limit) {
      await Promise.race(executing);
    }
  }
  return Promise.all(results);
}

async function fetchActivityViaSearch(page, username) {
  const baseSearch = `https://www.reddit.com/r/${SUBREDDIT}/search/?q=author%3A${username}`;
  const collected = [];

  for (const type of ['posts', 'comments']) {
    const url = `${baseSearch}&type=${type}`;
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await randomDelay(2000);
    const html = await page.evaluate(() => document.documentElement.outerHTML);
    fs.writeFileSync(path.join(HTML_DIR, `debug-${type}-${username}.html`), html);
  }

  for (const type of ['posts', 'comments']) {
    const filePath = path.join(HTML_DIR, `debug-${type}-${username}.html`);
    const html = fs.readFileSync(filePath, 'utf8');
    const $ = cheerio.load(html);

    $('search-telemetry-tracker').each((_, el) => {
      const $el = $(el);
      const link = $el.find('a').attr('href');
      const text = type === 'posts'
        ? $el.find('a[aria-label]').attr('aria-label')
        : $el.find('p').text().trim();

      if (link && text) {
        collected.push({ url: `https://www.reddit.com${link}`, text });
      }
    });
  }

  return collected;
}

async function fetchModLogContent(page, username) {
  const url = `https://www.reddit.com/mod/${SUBREDDIT}/log?pageSize=100&authorUsername=${username}`;
  await page.goto(url, { waitUntil: 'domcontentloaded' });

  const step1 = await page.waitForSelector('mod-log-page', { visible: true });
  const step2 = await step1.evaluateHandle(el => el.shadowRoot.querySelector('mod-log-username-filter'));

  await frontLock.acquire(); // Only one tab in front at a time
  await page.bringToFront();
  await randomDelay(100);
  await step2.click();
  const step4 = await step2.evaluateHandle(el => el.shadowRoot.querySelector('faceplate-form button[data-testid="done-btn"]'));
  await step4.click();
  frontLock.release();

  await randomDelay(2000);

  const html = await page.evaluate(() => document.documentElement.outerHTML);
  const htmlPath = path.join(HTML_DIR, `debug-modlog-${username}.html`);
  fs.writeFileSync(htmlPath, html);

  const loadedHtml = fs.readFileSync(htmlPath, 'utf8');
  const $ = cheerio.load(loadedHtml);
  const entries = [];

  $('table.mod-log-table tbody tr').each((_, el) => {
    const tds = $(el).find('td');
    const action = $(tds[3]).text().trim();
    const content = $(tds[4]).text().trim();
    if ((action === 'Remove link' || action === 'Remove comment') && content) {
      entries.push({ url, text: content, modlog: 1 });
    }
  });

  return entries;
}

async function evaluateUserForUnban(browser, username, reason) {
  await frontLock.acquire(); // One tab at a time for creation
  const page = await browser.newPage();
  await randomDelay(100);
  frontLock.release();

  const francModule = await import('franc-min');
  const franc = francModule.franc;
  const cleanedReason = reason.replace(/\s*\n\s*/g, ' ').trim();
  const userUrl = `https://www.reddit.com/user/${username}`;

  if (!cleanedReason.toLowerCase().includes(TARGET_REASON.toLowerCase())) {
    await page.close();
    return {
      username,
      reason: cleanedReason,
      note: 'Ban unrelated to language rule.',
      unban: 0,
      userUrl,
      contentUrls: '',
      contentChecks: ''
    };
  }

  const activity = await fetchActivityViaSearch(page, username);
  const modLog = await fetchModLogContent(page, username);

  const contentChecksRaw = activity.map(entry => {
    const lang = franc(entry.text);
    return {
      url: entry.url,
      sampled_text: entry.text,
      lang_detected: lang,
      is_dutch: lang === 'nld' || lang === 'und'
    };
  });

  const urlMap = new Map();
  for (const entry of contentChecksRaw) {
    const match = entry.url.match(/comments\/([a-z0-9]+)\/[^/]+(\/[a-z0-9]+)?/);
    if (!match) continue;
    const threadId = match[1];
    const isComment = !!match[2];
    const existing = urlMap.get(threadId);
    if (!existing || (!existing.isComment && isComment)) {
      urlMap.set(threadId, { ...entry, isComment });
    }
  }

  const contentChecks = Array.from(urlMap.values());
  const allContent = [...contentChecks, ...modLog];
  const uniqueByFullUrl = Array.from(new Map(allContent.map(item => [item.url, item])).values());

  const contentUrls = uniqueByFullUrl.map(c => c.url).join('; ');
  const dutchCount = uniqueByFullUrl.filter(entry => entry.is_dutch).length;
  const total = uniqueByFullUrl.length;
  const dutchRatio = total > 0 ? (dutchCount / total) * 100 : 0;

  let note = '';
  if (total < 5) note = 'Low activity; unban recommended.';
  else if (dutchRatio >= 70) note = '70%+ Dutch activity; unban recommended.';
  else note = 'Mostly non-Dutch activity; keep ban.';
  if (modLog.length > 0) note += ` | ModLog mentions: ${modLog.length}`;

  await page.close();

  return {
    username,
    reason: cleanedReason,
    note,
    unban: (total < 5 || dutchRatio >= 70) ? 1 : 0,
    userUrl,
    contentUrls,
    contentChecks: JSON.stringify(uniqueByFullUrl, null, 2)
  };
}

(async () => {
  const browser = await puppeteer.launch({ headless: false, defaultViewport: null, args: ['--start-maximized'] });
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
  await smartLogin(page);
  await page.goto(BANLIST_URL, { waitUntil: 'domcontentloaded' });

  let isFirstWrite = true;
  const seenUsers = new Set();
  let pageIndex = 0;

  while (pageIndex < PAGE_LIMIT) {
    let index = 0;
    const usersToEvaluate = [];

    while (true) {
      const usernameSelector = `div[slot="USERNAME${index}"] a[href^="/user/"]`;
      const reasonSelector = `div[slot="REASON${index}"]`;
      const usernameHandle = await page.$(usernameSelector);
      const reasonHandle = await page.$(reasonSelector);
      if (!usernameHandle || !reasonHandle) break;

      const username = await page.evaluate(el => el.textContent.trim().replace(/^u\//, ''), usernameHandle);
      const reason = await page.evaluate(el => el.innerText.trim(), reasonHandle);

      if (!seenUsers.has(username)) {
        usersToEvaluate.push({ username, reason });
        seenUsers.add(username);
      }
      index++;
    }

    const pageResults = await processWithConcurrencyPool(usersToEvaluate, CONCURRENCY_LIMIT, async ({ username, reason }) => {
      try {
        const evaluation = await evaluateUserForUnban(browser, username, reason);
        log(`✔️ Evaluated: ${evaluation.username} — Unban: ${evaluation.unban}`);
        return evaluation;
      } catch (err) {
        log(`❌ Error evaluating ${username}: ${err.message}`);
        return {
          username,
          reason,
          note: 'Error during evaluation',
          unban: 0,
          userUrl: '',
          contentUrls: '',
          contentChecks: ''
        };
      }
    });

    const fullWriter = createCsvWriter({
      path: path.join(RESULT_DIR, 'language_ban_review.csv'),
      header: [
        { id: 'username', title: 'Username' },
        { id: 'reason', title: 'Ban Reason' },
        { id: 'note', title: 'Note' },
        { id: 'unban', title: 'Unban' },
        { id: 'userUrl', title: 'User URL' },
        { id: 'contentUrls', title: 'Checked Content URLs' },
        { id: 'contentChecks', title: 'Content Checks (JSON)' }
      ],
      append: !isFirstWrite
    });

    const unbanWriter = createCsvWriter({
      path: path.join(RESULT_DIR, 'unban_only.csv'),
      header: [
        { id: 'username', title: 'Username' },
        { id: 'reason', title: 'Ban Reason' },
        { id: 'note', title: 'Note' },
        { id: 'unban', title: 'Unban' },
        { id: 'userUrl', title: 'User URL' },
        { id: 'contentUrls', title: 'Checked Content URLs' },
        { id: 'contentChecks', title: 'Content Checks (JSON)' }
      ],
      append: !isFirstWrite
    });

    await fullWriter.writeRecords(pageResults);
    await unbanWriter.writeRecords(pageResults.filter(u => u.unban === 1));
    isFirstWrite = false;

    log(`📄 Page ${pageIndex + 1}: wrote ${pageResults.length} users to CSV`);

    try {
      const paginator = await page.waitForSelector('user-management-pagination', { visible: true });
      const nextButtonHandle = await paginator.evaluateHandle(el => el.shadowRoot.querySelector('button.paginate-next-btn'));
      if (!nextButtonHandle || await nextButtonHandle.evaluate(btn => btn.disabled)) break;
      await nextButtonHandle.click();
      await randomDelay(2000);
      await page.waitForSelector('div[slot^="USERNAME0"]', { timeout: 10000 });
      pageIndex++;
    } catch {
      break;
    }
  }

  log('✅ Done!');
  await browser.close();
})();
