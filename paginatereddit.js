require('dotenv').config();
const fs = require('fs');
const snoowrap = require('snoowrap');

const reddit = new snoowrap({
  userAgent: process.env.REDDIT_USER_AGENT,
  clientId: process.env.REDDIT_CLIENT_ID,
  clientSecret: process.env.REDDIT_CLIENT_SECRET,
  username: process.env.REDDIT_USERNAME,
  password: process.env.REDDIT_PASSWORD
});

async function paginateFunny(subredditName = 'funny', pages = 3, itemsPerPage = 25) {
  let allVideoUrls = [];
  let after = null;

  try {
    for (let page = 1; page <= pages; page++) {
      const listing = await reddit.getSubreddit(subredditName).getHot({ limit: itemsPerPage, after });

      console.log(`\n--- Page ${page} ---`);
      listing.forEach((post, index) => {
        const isVideo = post.is_video || (post.media && post.media.reddit_video);
        if (isVideo) {
          const videoUrl = post.media?.reddit_video?.fallback_url || post.url;
          console.log(`${index + 1}. ${post.title} -> ${videoUrl}`);
          allVideoUrls.push(videoUrl);
        }
      });

      after = listing[listing.length - 1]?.name;
      if (!after) break; // no more pages
    }

    // Save to file
    fs.writeFileSync('testurls.txt', allVideoUrls.join('\n'), 'utf8');
    console.log(`\n✅ Saved ${allVideoUrls.length} video URLs to testurls.txt`);

  } catch (err) {
    console.error('❌ Error fetching subreddit data:', err.message);
  }
}

paginateFunny();
