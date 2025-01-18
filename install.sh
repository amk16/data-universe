
######## Add redis repo ########
curl -fsSL https://packages.redis.io/gpg | gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
ret=$?
if [[ $ret -ne 0 ]]; then
    echo "failed to add redis repo." | tee -a "$INSTALL_LOG"
fi
echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/redis.list


####### Ubuntu packages ########

echo 'installing ubuntu packages'
apt -y update && apt -y install redis taskiq taskiq-redis

######## Install npm packages ########
echo 'installing pm2'
npm install -g pm2

######## Install python packages ########
echo 'installing python packages'
python -m pip install -e.


######## Parallelization setup ########
echo 'starting redis-server'
redis-server --daemonize yes

echo 'starting taskiq worker'
nohup taskiq worker scraping.reddit.parallel_reddit_scraper:broker -- workers 10 --max-async-tasks 1 >> "$DOWNLOAD_LOG_FILE" 2>&1 &





