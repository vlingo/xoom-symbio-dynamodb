# vlingo-symbio-dynamodb
Implementation of vlingo-symbio for Amazon AWS DynamoDB

[![Javadocs](http://javadoc.io/badge/io.vlingo/vlingo-symbio-dynamodb.svg?color=brightgreen)](http://javadoc.io/doc/io.vlingo/vlingo-symbio-dynamodb) [![Build Status](https://travis-ci.org/vlingo/vlingo-symbio-dynamodb.svg?branch=master)](https://travis-ci.org/vlingo/vlingo-symbio-dynamodb) [ ![Download](https://api.bintray.com/packages/vlingo/vlingo-platform-java/vlingo-symbio-dynamodb/images/download.svg) ](https://bintray.com/vlingo/vlingo-platform-java/vlingo-symbio-dynamodb/_latestVersion)

### State Storage
The `StateStore` is a simple object storage mechanism that can be run against a number of persistence engines.
Available JDBC storage implementations:

   - DynamoDB Text Store: `DynamoDBTextStateActor`
   - DynamoDB Binary Store: `DynamoDBBinaryStateActor`

We welcome you to add support for your favorite database!

### Bintray

```xml
  <repositories>
    <repository>
      <id>jcenter</id>
      <url>https://jcenter.bintray.com/</url>
    </repository>
  </repositories>
  <dependencies>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio</artifactId>
      <version>0.7.7</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio-dynamodb</artifactId>
      <version>0.7.7</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
```

```gradle
dependencies {
    compile 'io.vlingo:vlingo-symbio:0.7.7'
    compile 'io.vlingo:vlingo-symbio-dynamodb:0.7.7'
}

repositories {
    jcenter()
}
```

License (See LICENSE file for full license)
-------------------------------------------
Copyright © 2012-2018 Vaughn Vernon. All rights reserved.

This Source Code Form is subject to the terms of the
Mozilla Public License, v. 2.0. If a copy of the MPL
was not distributed with this file, You can obtain
one at https://mozilla.org/MPL/2.0/.
