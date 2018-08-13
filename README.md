<p align="center">
  <img src="https://github.com/crahal/centgovspend/blob/master/compile/figures/timeline.png" width="900"/>
</p>

# centgovspend

[![Generic badge](https://img.shields.io/badge/Python-3.6-<red>.svg)](https://shields.io/)  [![Generic badge](https://img.shields.io/badge/License-MIT-blue.svg)](https://shields.io/)  [![Generic badge](https://img.shields.io/badge/Maintained-Yes-green.svg)](https://shields.io/)

> Please note: this is an extremely preliminary version of the library (v.0.1.0).

> There are a _lot_ of features to add and creases to iron out.

This is a repo for scraping, parsing and automatically reconciling ministerial and non-ministerial transparency spending data above £25k at the granularized payment level. For some background reading on the data origination and provenance, [please see this document](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/662332/guidance_for_publishing_spend.pdf). It aggregates and cleans thousands of files (mostly csv, xls and xlsx) for consistency and quality controls the files in various ways. The files are predominantly hosted on gov.uk and data.gov.uk, although some are hosted on departmental homepages and require more specialised functions. The library will be quarterly updated to make sure all new files are being captured, although there will invariably be some holes due to HTTP 404s appearing over time. Please feel free to raise issues or pull requests with any updates. An An auxiliary function matches the supplier field via the [OpenCorporates REST API](https://api.opencorporates.com/documentation/API-Reference) (bypassing Open Refine). It then links *any* positive matches found with the (Companies House API)[https://developer.companieshouse.gov.uk/api/docs/index.html] and builds in a range of supplementary data on the matched company. The spending data involved is made available under an [Open Government License](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/). The [OpenCorporates reconciliations](https://opencorporates.com) for company names are made available under an [ODbl License](https://opencorporates.com/info/licence). [Companies House data](http://business.data.gov.uk/companies/docs/about-this-service.html) is supplied under section 47 and 50 of the Copyright, Designs and Patents Act 1988 and Schedule 1 of the Database Regulations (SI 1997/3032). You'll need a Companies House API key which should be stored in src/ch_apikey (or use the ```noreconcile``` option described below).

#### Install and Run

The only requirement is an installation of Python 3, and the only dependancies are [unidecode](https://pypi.org/project/Unidecode/), [BeautifuLSoup4](https://pypi.org/project/beautifulsoup4/) and [ratelimit](https://pypi.org/project/ratelimit/). To run: [download a zip](https://github.com/crahal/centgovspend/archive/master.zip) of this repository or `git clone github.com/crahal/centgovspend/`. All that's then required is running

```python centgovspend.py [options]```

 at the command line. For debugging, a logfile can be found at logging/centgovspend.log.

#### Options

There are a range of options (all optional) to include in the execution of the above command, mostly aimed at reducing repetition on repeat runs or focusing on specific datasets:

* `depttype=ministerial`   : only scrape/parse ministerial depts

* `depttype=nonministerial` : only scrape/parse nonministerial depts

* `cleanrun`                : delete all subdirectories before running (default = off)

* `noscrape`                : dont scrape any new data (incompatibile with cleanrun, default off)

* `noreconcile`        : don't reconcile via opencorporates and companieshouse (default = do it)

#### Reconciliations and Summary Statistics

```
We matched 1973950 out of 2751185 payments in total (71.75%).
We matched £213552680169 out of £1003640600205 value in total (21%).
We matched 27551 out of 60611 unique suppliers in total (45.46%).
```

The file ```clean_matches.py``` provides a fuction for post-processing of the matches. The first option ( ```type == 'automated_safe'```) automatically accepts any match which has a score greater than 70 and which does not have a second highest match score within 10 points of it.  The second option (```type == 'manual_verification'```) automatically rejects all matches below a score of 20, accepts all above 70, and asks for manual verification of those inbetween.

<p align="center">
  <img src="https://github.com/crahal/centgovspend/blob/master/compile/figures/mostmatch_and_safematch.png" width="700"/>
</p>

#### To Do

1. Finish the centgovspend.ipynb to visualise the database.
2. Build a dashboard for visualisations.
3. Upload a presentation.tex into Compile/Presentations.
4. Put a preprint up onto SocArXiv.

*Next update of scrapers: 15/11/2018*


#### License
This work is free. You can redistribute it and/or modify it under the terms of the MIT license. This license does not apply to any input or output data processed.
