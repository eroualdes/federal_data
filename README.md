# `fd`

fd is a command-line application that provides analysis ready federal data: one part [Quandl](http://www.quandl.com/) + one part [DATA.GOV](http://www.data.gov/) + APIs aren't for everybody = data in [CSV](http://en.wikipedia.org/wiki/Comma-separated_values) format.  Currently, this project downloads only some of the available data from a select group of federal agencies.

Clone repository and build with `lein uberjar`.  Print help screen with

```
$ java -jar target/fd.jar --help
```

or read better [documentation](roualdes.us/docs).

## License
Copyright © 2015 Edward A. Roualdes
Distributed under the EPL-1.0.

