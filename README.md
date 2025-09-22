# CricBase
Documenting my journey building an ETL pipeline for cricsheet ball-by-ball cricket data for use in modelling and analysis.

Equipped with a love of sabermetrics and all things sport, there was only so much data collection I could do in my job before I inevitably wanted to get my hands on some cricket data myself. Finding accessible, high-quality ball-by-ball data proved to be a significantly harder task than I expected. I settled on using data provided by Cricsheet (https://cricsheet.org/), a site inspired by Retrosheet that provides similar data for Major League Baseball. This repository serves as an organised collection of a small fraction of the work done on this project over the last year.

I've focused on ICC-sanctioned men's and women's T20 international cricket because of the uniformity in rules across matches (no subs, funky powerplay changes etc.). I do anticipate expansion over time. Cricsheet (quite admirably) don't include Afghanistan matches in their available database due to their ongoing treatment of their women's team; however, I do intend to add their information at some point for completeness. But for similar reasons to Cricsheet, it's not a priority for me.

Tools used:
 - Python (Including packages: pandas, sqlite3, requests,...)
 - SQL

This data is made available under the [Open Data Commons Attribution License (ODC-By) v1.0](https://opendatacommons.org/licenses/by/1-0/)
