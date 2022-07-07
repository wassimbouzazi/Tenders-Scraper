# main.py

from flask import Flask
import os
import subprocess

app = Flask(__name__)

# GCP Cloud Run requires that the app listens at "/" while it is working, to respond to health checks
@app.route("/")
def main():
    return "Works"

# Scans the website for new notices to schedule them for later scraping
@app.route("/scan")
def scan():
    exec(open("scanner.py").read())
    return "Finished scheduling scrapes successfully."

# Scrapes the given notice id
@app.route("/scrape/<id>/<timestamp>")
def scrape(id, timestamp):
    command = "python3 scraper.py {} {}".format(id, timestamp)
    subprocess.call([command], shell=True)
    return "Finished scraping #{} published on {}".format(id, timestamp)
