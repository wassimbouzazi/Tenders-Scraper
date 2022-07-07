# Use the official Python image.
# https://hub.docker.com/_/python
FROM python:3.9

# Install manually all the missing libraries
RUN apt-get update
RUN apt-get install -y gconf-service libasound2 libatk1.0-0 \
    libcairo2 libcups2 libfontconfig1 libgdk-pixbuf2.0-0 \
    libgtk-3-0 libnspr4 libpango-1.0-0 libxss1 \
    libappindicator3-1 libgbm1 \
    fonts-liberation libappindicator1 libnss3 lsb-release xdg-utils ||true
    

# Install Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN dpkg -i google-chrome-stable_current_amd64.deb; apt-get -fy install

# Install Python dependencies.
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . .

# Variables
CMD export DATABASE_HOST="35.246.84.160:5432/tenders1"
CMD export BASEURL="https://tenders-scraper-myazracxxq-nw.a.run.app/scrape/"


# Run the web service on container startup - this is necessary to satisfy GCP Cloud Run contract
CMD exec gunicorn --bind :$PORT main:app --timeout 90