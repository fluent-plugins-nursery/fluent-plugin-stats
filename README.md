# fluent-plugin-calc [![Build Status](https://secure.travis-ci.org/sonots/fluent-plugin-calc.png?branch=master)](http://travis-ci.org/sonots/fluent-plugin-calc) [![Dependency Status](https://gemnasium.com/sonots/fluent-plugin-calc.png)](https://gemnasium.com/sonots/fluent-plugin-calc)

Simple fluentd plugin to calculate messages.

## Configuration

Example (sum for xxx_count, max for xxx_max, min for xxx_min, avg for xxx_avg): 

    <match foo.**>
      type calc
      interval 5s
      add_tag_prefix calc

      sum .*_count$
      max .*_max$
      min .*_min$
      avg .*_avg$
    </match>

Assuming following inputs are coming:

    foo.bar: {"4xx_count":1,"5xx_count":2","reqtime_max":12083,"reqtime_min":10,"reqtime_avg":240.46}
    foo.bar: {"4xx_count":4,"5xx_count":2","reqtime_max":24831,"reqtime_min":82,"reqtime_avg":300.46}

then output bocomes as belows: 

    calc.foo.bar: {"4xx_count":5,"5xx_count":4","reqtime_max":24831,"reqtime_min":10,"reqtime_avg":270.46}

## Parameters

- sum, min, max, avg

    Calculation. Specify input keys by a regular expression

- interval

    The interval to calculate in seconds. Default is 5s. 

- tag

    The output tag name. Required for aggregate `all`. 

- add_tag_prefix

    Add tag prefix for output message. 

- aggragate
    
    Calculate by each `tag` or `all`. The default value is `tag`.

- store_file

    Store internal data into a file of the given path on shutdown, and load on starting.

## ChangeLog

See [CHANGELOG.md](CHANGELOG.md) for details.

## ToDo

Get the number of denominator to calculate `avg` from input json field. 

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new [Pull Request](../../pull/new/master)

## Copyright

Copyright (c) 2013 Naotoshi SEO. See [LICENSE](LICENSE) for details.

