# fluent-plugin-stats [![Build Status](https://secure.travis-ci.org/sonots/fluent-plugin-stats.png?branch=master)](http://travis-ci.org/sonots/fluent-plugin-stats)

Fluentd plugin to calculate statistics such as sum, max, min, avg.

## Configuration

### Example 1

Get sum for xxx\_count, max for xxx\_max, min for xxx\_min, avg for xxx\_avg

    <match foo.**>
      type stats
      interval 5s
      add_tag_prefix stats

      sum .*_count$
      max .*_max$
      min .*_min$
      avg .*_avg$
    </match>

Assuming following inputs are coming:

    foo.bar: {"4xx_count":1,"5xx_count":2","reqtime_max":12083,"reqtime_min":10,"reqtime_avg":240.46}
    foo.bar: {"4xx_count":4,"5xx_count":2","reqtime_max":24831,"reqtime_min":82,"reqtime_avg":300.46}

then output bocomes as belows: 

    stats.foo.bar: {"4xx_count":5,"5xx_count":4","reqtime_max":24831,"reqtime_min":10,"reqtime_avg":270.46}

### Example 2

Get sum, max, min, avg for the same key

    <match foo.**>
      type stats
      interval 5s
      add_tag_prefix stats

      sum ^reqtime$
      max ^reqtime$
      min ^reqtime$
      avg ^reqtime$
      sum_suffix _sum
      max_suffix _max
      min_suffix _min
      avg_suffix _avg
    </match>

Assuming following inputs are coming:

    foo.bar: {"reqtime":1.000}
    foo.bar: {"reqtime":2.000}

then output bocomes as belows: 

    stats.foo.bar: {"reqtime_sum":3.000,"reqtime_max":2.000,"reqtime_min":1.000,"reqtime_avg":1.500}

## Parameters

- sum, min, max, avg

    Target of calculation. Specify input keys by a regular expression

- sum\_keys, min\_keys, max\_keys, avg\_keys

    Target of calculation. Specify input keys by a string separated by , (comma) such as

        sum_keys 4xx_count,5xx_count

- sum\_suffix, min\_suffix, max\_suffix, avg\_suffix

    Add a suffix to keys of the output record

- interval

    The interval to calculate in seconds. Default is 5s. 

- tag

    The output tag name. Required for aggregate `all`. 

- add_tag_prefix

    Add tag prefix for output message. Default: 'stats'

- remove_tag_prefix

    Remove tag prefix for output message. 

- aggragate
    
    Calculate by each `tag` or `all`. The default value is `tag`.

- store_file

    Store internal data into a file of the given path on shutdown, and load on starting.

- zero_emit

    Emit 0 on the next interval. This is useful for some software which requires to reset data such as [GrowthForecast](http://kazeburo.github.io/GrowthForecast/) . 

        stats.foo.bar: {"4xx_count":5,"5xx_count":4","reqtime_max":24831,"reqtime_min":10,"reqtime_avg":270.46}
        # after @interval later
        stats.foo.bar: {"4xx_count":0,"5xx_count":0","reqtime_max":0,"reqtime_min":0,"reqtime_avg":0}

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

Copyright (c) 2013 Naotoshi Seo. See [LICENSE](LICENSE) for details.

