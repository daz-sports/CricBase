# CricBase
Documenting my journey building an ETL pipeline for cricsheet ball-by-ball cricket data for use in modelling and analysis.

*Status Update:* I am taking a break from producing modelling output to focus on increasing the size and complexity of the database. This expansion will enable me to utilise more advanced modelling and machine learning techniques to study aspects such as the expected run value of wickets or principal component analysis to classify batter and bowler types.

#### Project Background
Equipped with a love of sabermetrics and all things sport, there was only so much data collection I could do in my job before I inevitably wanted to get my hands on some cricket data myself. Finding accessible, high-quality ball-by-ball data proved to be a significantly harder task than I expected. I settled on using data provided by Cricsheet (https://cricsheet.org/), a site inspired by Retrosheet that provides similar data for Major League Baseball. This repository serves as an organised collection of a small fraction of the work done on this project over the last year.

#### Project Details
I've focused on ICC-sanctioned men's and women's T20 international cricket because of the uniformity in rules across matches (no subs, funky powerplay changes, etc.). I do anticipate expansion over time. Cricsheet (quite admirably) doesn't include Afghanistan matches in their available database due to their ongoing treatment of their women's team; however, I do intend to add their information at some point for completeness. But for similar reasons to Cricsheet, it's not a priority for me.

The code required to build the database is in the 'scripts' folder, and the sample data is in the 'data' folder. The 'data' folder contains the JSON files provided by Cricsheet for all T20 matches played between full member nations (excluding Afghanistan) in 2024 (male and female), along with the relevant venue, player, officials, and team profiles required to create a complete sample of the database. Also provided is the relevant code to add more Cricsheet T20 international matches should you wish.

The 'notebooks' folder contains a selection of analyses possible with this sample of the database. Below is the link to view these Jupyter notebooks in interactive form (previews available in the 'notebooks' folder):

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/daz-sports/CricBase/main?urlpath=lab/tree/notebooks)

The database contains match information, ball-by-ball data for each innings, and players for each match. This is supplemented by player, match official, venue, and team profiles (data collected manually from espncricinfo.com). It also contains a record of official T20 international matches not included in the database. Finally, it includes some metadata-related tables.

Tools used:
 - Python
   - Standard library packages:
      - calendar
      - contextlib
      - dataclasses
      - datetime
      - json
      - logging
      - os
      - sqlite3
      - typing
      - urllib
      - webbrowser
   - Third-party packages:
      - matplotlib
      - numpy
      - pandas
      - requests
      - seaborn
      - statsmodels
 - SQLite

#### Database Construction Instructions
1. Clone this repository.
2. Install the required packages listed above.
3. Fill in the 'config.py' file.
3. Run the 'running.py' script in the 'scripts' folder. Split screen your IDE with a web browser if possible.
4. Explore the database!

The Cricsheet data is made available under the [Open Data Commons Attribution Licence (ODC-By) v1.0](https://opendatacommons.org/licenses/by/1-0/)
Missing matches discovery assisted by public record data from the ICC schedule.

*Note on the ICC scraper*:
This tool is intended for research and educational purposes only. It uses publicly available API endpoints to find data gaps in the database. Please respect the ICC's infrastructure by maintaining a reasonable rate limit.