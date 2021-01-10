## Install
```
mvn package
```

or, without test:

```
mvn package -DskipTests
```

```
ls ./target/marcotollini.kafkaconnect.nfacctd.utils-1.0.jar
```

## Usage by example - RegexNullify
The transformation applies only on String or OptionalString and will ALWAYS return an OptionalString. If the field is not String, it requires a Cast stransformation.

`.regex` is used in String.matches in java (https://docs.oracle.com/javase/7/docs/api/java/lang/String.html#matches(java.lang.String)). Anything supported by that is also supported by RegexNullify.

`.fields` is one or more fields, comma-separated

```
"config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": 1,
    "connection.url": "jdbc:postgresql://postgres:5432/l3visualization?user=l3visualization&password=postgres_password",
    "topics": "nfacctd_bmp",
    "table.name.format": "event_init",
    "transforms": "filtermsg,timestamp0000",
    "transforms.filtermsg.type": "io.confluent.connect.transforms.Filter$Value",
    "transforms.filtermsg.filter.condition": "$[?(@.event_type == \"log\" && @.bmp_msg_type == \"init\")]",
    "transforms.filtermsg.filter.type": "include",
    "transforms.timestamp0000.type": "com.github.marcotollini.kafka.connect.nfacctd.smt.RegexNullify$Value",
    "transforms.timestamp0000.fields": "timestamp,timestamp_event,timestamp_arrival",
    "transforms.timestamp0000.regex": "0000-00-00T00:00:00.000000Z"
}
```

## Usage by example - ToJsonList
Takes a String separated by some value, and returns a valid JSON array.
Example: "1 2 3 4" => ["1", "2", "3", "4"] with space as separator.

`.fields` is one or more fields, comma-separated

`.separator` is used togheter with String.split (https://docs.oracle.com/javase/7/docs/api/java/lang/String.html#split(java.lang.String)) and can use any regex. Kafka connect does not permit " " as a valid separator, thus the wild card "\\space" has been created.

```
"config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": 1,
    "connection.url": "jdbc:postgresql://postgres:5432/l3visualization?user=l3visualization&password=postgres_password",
    "topics": "nfacctd_bmp",
    "table.name.format": "event_route_monitor",
    "transforms": "filtermsg,timestamp0000,castbool,tojson",
    "transforms.filtermsg.type": "io.confluent.connect.transforms.Filter$Value",
    "transforms.filtermsg.filter.condition": "$[?(@.event_type == \"log\" && @.bmp_msg_type == \"route_monitor\")]",
    "transforms.filtermsg.filter.type": "include",
    "transforms.timestamp0000.type": "com.github.marcotollini.kafka.connect.nfacctd.smt.RegexNullify$Value",
    "transforms.timestamp0000.fields": "timestamp,timestamp_arrival",
    "transforms.timestamp0000.regex": "0000-00-00T00:00:00.000000Z",
    "transforms.castbool.type": "org.apache.kafka.connect.transforms.Cast$Value",
    "transforms.castbool.spec": "is_in:boolean,is_filtered:boolean,is_loc:boolean,is_post:boolean,is_out:boolean",
    "transforms.tojson.type": "com.github.marcotollini.kafka.connect.nfacctd.smt.ToJsonList$Value",
    "transforms.tojson.fields": "comms,ecomms,lcomms,as_path",
    "transforms.tojson.separator": "\\space"
}
```