# vlingo-symbio-dynamodb

[![Javadocs](http://javadoc.io/badge/io.vlingo/vlingo-symbio-dynamodb.svg?color=brightgreen)](http://javadoc.io/doc/io.vlingo/vlingo-symbio-dynamodb) [![Build](https://github.com/vlingo/vlingo-symbio-dynamodb/workflows/Build/badge.svg)](https://github.com/vlingo/vlingo-symbio-dynamodb/actions?query=workflow%3ABuild) [ ![Download](https://api.bintray.com/packages/vlingo/vlingo-platform-java/vlingo-symbio-dynamodb/images/download.svg) ](https://bintray.com/vlingo/vlingo-platform-java/vlingo-symbio-dynamodb/_latestVersion) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/vlingo-platform-java/symbio)

The VLINGO XOOM platform SDK implementation of XOOM SYMBIO for Amazon AWS DynamoDB.

Docs: https://docs.vlingo.io/vlingo-symbio

### State Storage
The `StateStore` is a simple object storage mechanism that can be run against a number of persistence engines.
Available JDBC storage implementations:

   - DynamoDB Text Store: `DynamoDBTextStateActor`
   - DynamoDB Binary Store: `DynamoDBBinaryStateActor`

### Installation

```xml
  <dependencies>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio</artifactId>
      <version>1.5.2</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio-dynamodb</artifactId>
      <version>1.5.2</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
```

```gradle
dependencies {
    compile 'io.vlingo:vlingo-symbio:1.5.2'
    compile 'io.vlingo:vlingo-symbio-dynamodb:1.5.2'
}
```

License (See LICENSE file for full license)
-------------------------------------------
Copyright © 2012-2020 VLINGO LABS. All rights reserved.

This Source Code Form is subject to the terms of the
Mozilla Public License, v. 2.0. If a copy of the MPL
was not distributed with this file, You can obtain
one at https://mozilla.org/MPL/2.0/.
