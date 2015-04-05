# Introduction to federal_data

federal_data is a command-line application written in [Clojure](http://clojure.org/).  The important file is fd.jar which downloads select federal agencies' freely available data (and in the future will make it analysis ready data).  Java (>=1.6) is required; try `$ java -version`.  Use fd.jar by calling

```
$ java -jar fd.jar action -a AGENCY -f /abs/path/to/folder
```

## Available Actions

federal_data can download data from the following agencies
* [MSHA](http://www.msha.gov/STATS/PART50/p50y2k/p50y2k.HTM)
* [BLS](http://www.bls.gov/cew/datatoc.htm) (qcew data)
* [NOAA](http://www.ncdc.noaa.gov/data-access)
* [EPA](http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/download_files.html)

Alternatively, you could run

```
$ java -jar fd.jar available
```

to see which federal agencies are currently available, or

```
$ java -jar fd.jar available -a AGENCY
```

to get a list of the data sets available for a particular federal agency.

Then,

```
$ java -jar fd.jar download -a AGENCY -f /path/to/msha/folder
```

will get you some data from `AGENCY`.