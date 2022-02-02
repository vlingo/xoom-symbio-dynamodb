# xoom-symbio-dynamodb

[![Javadocs](http://javadoc.io/badge/io.vlingo.xoom/xoom-symbio-dynamodb.svg?color=brightgreen)](http://javadoc.io/doc/io.vlingo.xoom/xoom-symbio-dynamodb) [![Build](https://github.com/vlingo/xoom-symbio-dynamodb/workflows/Build/badge.svg)](https://github.com/vlingo/xoom-symbio-dynamodb/actions?query=workflow%3ABuild) [![Download](https://img.shields.io/maven-central/v/io.vlingo.xoom/xoom-symbio-dynamodb?label=maven)](https://search.maven.org/artifact/io.vlingo.xoom/xoom-symbio-dynamodb) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/vlingo-platform-java/symbio)

The VLINGO XOOM platform SDK implementation of XOOM SYMBIO for Amazon AWS DynamoDB.

Docs: https://docs.vlingo.io/xoom-symbio

### State Storage
The `StateStore` is a simple object storage mechanism that can be run against a number of persistence engines.
Available JDBC storage implementations:

   - DynamoDB Text Store: `DynamoDBTextStateActor`
   - DynamoDB Binary Store: `DynamoDBBinaryStateActor`

### Installation

```xml
  <dependencies>
    <dependency>
      <groupId>io.vlingo.xoom</groupId>
      <artifactId>xoom-symbio</artifactId>
      <version>1.9.1</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>io.vlingo.xoom</groupId>
      <artifactId>xoom-symbio-dynamodb</artifactId>
      <version>1.9.1</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
```

```gradle
dependencies {
    compile 'io.vlingo.xoom:xoom-symbio:1.9.1'
    compile 'io.vlingo.xoom:xoom-symbio-dynamodb:1.9.1'
}
```

License (See LICENSE file for full license)
-------------------------------------------
Copyright © 2012-2021 VLINGO LABS. All rights reserved.

This Source Code Form is subject to the terms of the
Mozilla Public License, v. 2.0. If a copy of the MPL
was not distributed with this file, You can obtain
one at https://mozilla.org/MPL/2.0/.
