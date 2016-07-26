# fd: analysis ready US federal data

fd is a command line application that provides analysis ready US federal data: one part [Quandl](http://www.quandl.com/) + one part [DATA.GOV](http://www.data.gov/) + APIs aren't for everybody = data in [CSV](http://en.wikipedia.org/wiki/Comma-separated_values) format.

Install `fd` with

```
$ pip install git+https://github.com/roualdes/fd
```

Currently, this project downloads only some of the available data from a select group of federal agencies.  Try

```
$ fd available
```

## Roadmap
* add more federal agencies
* documentation

## License
Copyright © 2016 Edward A. Roualdes
Distributed under the GPLv3.
