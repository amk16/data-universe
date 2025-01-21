pkill taskiq
nohup taskiq worker scraping.reddit.parallel_reddit_scraper:broker -- workers 10 --max-async-tasks 1 >> "$SCRAPING_LOG_FILE" 2>&1 &
