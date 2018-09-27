<p align="center">
  <img src="https://github.com/crahal/centgovspend/blob/master/compile/figures/timeline.png" width="900"/>
</p>

# centgovspend

[![Generic badge](https://img.shields.io/badge/Python-3.6-<red>.svg)](https://shields.io/)  [![Generic badge](https://img.shields.io/badge/License-MIT-blue.svg)](https://shields.io/)  [![Generic badge](https://img.shields.io/badge/Maintained-Yes-green.svg)](https://shields.io/)

> This is the first stable release (v.1.0.0) of the library. Please raise all issues as you find them, submit pull requests, etc! A link to the working paper which discusses this work can be found [here](https://osf.io/preprints/socarxiv/9c7m2/). Please get in touch if you would like to use this tool for your own purposes: I would be happy to help as required.

This is a repo for scraping, parsing and automatically reconciling ministerial and non-ministerial transparency spending data above £25k at the granularized payment level. For some background reading on the data origination and provenance, [please see this document](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/662332/guidance_for_publishing_spend.pdf). It aggregates and cleans thousands of files (mostly csv, xls, xlsx and .ods) for consistency and quality controls the files in various ways. The files are predominantly hosted on gov.uk and data.gov.uk, although some are hosted on departmental homepages and require more specialised functions. The library will be quarterly updated (the first day of each quarter: 1st of January, April, July and October) to make sure all new files are being captured, although there will invariably be some holes due to HTTP 404s appearing over time (these are caught and logged). An auxiliary function matches the supplier field via the [OpenCorporates REST API](https://api.opencorporates.com/documentation/API-Reference) (bypassing Open Refine). It then links _any_ positive matches found with the [Companies House API](https://developer.companieshouse.gov.uk/api/docs/index.html) and builds in a range of supplementary data on the matched company. The spending data involved is made available under an [Open Government License](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/). The [OpenCorporates reconciliations](https://opencorporates.com) for company names are made available under an [ODbl License](https://opencorporates.com/info/licence). [Companies House data](http://business.data.gov.uk/companies/docs/about-this-service.html) is supplied under sections 47 and 50 of the Copyright, Designs and Patents Act 1988 and Schedule 1 of the Database Regulations (SI 1997/3032). You'll need a Companies House API key which should be stored in src/ch_apikey (or use the `noreconcile` option described below).

#### Install and Run

The only requirement is an installation of Python 3, and the only dependancies are [unidecode](https://pypi.org/project/Unidecode/), [BeautifuLSoup4](https://pypi.org/project/beautifulsoup4/), [ratelimit](https://pypi.org/project/ratelimit/) and [easyodf](https://pypi.org/project/ezodf/). To run: [download a zip](https://github.com/crahal/centgovspend/archive/master.zip) of this repository or:

`git clone github.com/crahal/centgovspend/`

All that's then required is running:

`python centgovspend.py [options]`

at the command line. Note: we also include zips of the most recently reconciled data in `data/master`: unzip these files to dramatically improve execution time and to prevent multiple identical queries of the relevant APIs. Please respect the licenses under which this data is made available. For debugging, a logfile can be found at logging/centgovspend.log.

#### Options

There are a range of options (all optional) to include in the execution of the above command, mostly aimed at reducing repetition on repeat runs or focusing on specific datasets:

-   `depttype=ministerial`   : only scrape/parse ministerial depts

-   `depttype=nonministerial` : only scrape/parse nonministerial depts

-   `cleanrun`                : delete all subdirectories before running (default = off)

-   `noscrape`                : dont scrape any new data (incompatibile with cleanrun, default off)

-   `noreconcile`        : don't reconcile via opencorporates and companieshouse (default = do it)

-   `noevaluate`        : don't evaluate the scrape, mostly for debugging (default = do it)

#### Reconciliations and Summary Statistics

<img src="https://github.com/crahal/centgovspend/blob/master/compile/figures/mostmatch_and_safematch_a.png" width="400"/> <img src="https://github.com/crahal/centgovspend/blob/master/compile/figures/mostmatch_and_safematch_b.png" width="400"/>

The above figure shows the result of the reconciliations. The file `clean_matches.py` provides a fuction for post-processing of the matches. The first option ( `type == 'automated_safe'`) automatically accepts any match which has a score greater than 70 and which does not have a second highest match score within 10 points of it. The second option (`type == 'manual_verification'`) automatically rejects all matches below a score of 20, accepts all above 70, and asks for manual verification of those inbetween. An accompanying [jupyter notebook](https://github.com/crahal/centgovspend/tree/master/src/centgovspend_notebook.ipynb) undertakes the safematch and a range of analysis which is included in the accompanying paper.

    We matched 1973950 out of 2751185 payments in total (71.75%).
    We matched £213552680169 out of £1003640600205 value in total (21%).
    We matched 27551 out of 60611 unique suppliers in total (45.46%).

One example is a decomposition across departments and Standard Industry Classifiers (SICs):

<img src="https://github.com/crahal/centgovspend/blob/master/compile/figures/sic_dept_heatmap.png" width="800"/>

And a more sociological track which shows a range of analysis related to the officers and persons of significant control of companies which supply government compared with the entire population of Companies House (created with [these](https://github.com/crahal/centgovspend/tree/master/src/ch_supportfunctions.py) functions) more generally:

<img src="https://github.com/crahal/centgovspend/blob/master/compile/figures/psc_age.png" width="400"/> <img src="https://github.com/crahal/centgovspend/blob/master/compile/figures/officers_age.png" width="400"/>

_Next update of scrapers: 1/10/2018_

#### License

This work is free. You can redistribute it and/or modify it under the terms of the MIT license. This license does not apply to any input or output data processed.
