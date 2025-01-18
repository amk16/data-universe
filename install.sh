

SCRAPING_LOG_FILE="/workspace/data-universe/logs/scraping.log"



######## MAKE LOG FILE ########
echo 'making log file'
touch "$SCRAPING_LOG_FILE"


######## Add redis repo ########

sudo apt-get install lsb-release curl gpg
curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
sudo chmod 644 /usr/share/keyrings/redis-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list
sudo apt-get update
sudo apt-get install redis


####### Ubuntu packages ########

echo 'installing ubuntu packages'
apt -y update && apt -y install redis taskiq taskiq-redis

######## Install npm packages ########
echo 'installing pm2'
npm install -g pm2

######## Install python packages ########
echo 'installing python packages'
python -m pip install -e .



######## Verify installation ########
echo 'checking if module is importable'
python -c "from scraping.reddit.redis_config import broker; print('Module found')"


######## Parallelization setup ########
echo 'starting redis-server'
redis-server --daemonize yes


echo 'checking redis-server'
redis-cli ping

echo 'starting taskiq worker'
nohup taskiq worker scraping.reddit.redis_config:broker -- workers 10 --max-async-tasks 1 >> "$SCRAPING_LOG_FILE" 2>&1 &





