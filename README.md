# Callcenter Dashboard #

### 1) Add a `Credentials.scala` file in `src\main\scala\de\hpi\callcenterdashboard` with the following content:

```scala
package de.hpi.callcenterdashboard

class Credentials extends CredentialsTrait {
  val hostname = "side.eaalab.hpi.uni-potsdam.de"
  val username = "<-->"
  val password = "<-->"
  val port = 31815
}
```


### 2) Build & Run

Linux:
```sh
$ ./sbt
> ~;jetty:start;jetty:stop
```

Windows:
```
$ sbt
> ~;jetty:start;jetty:stop
```

Open [http://localhost:8080/](http://localhost:8080/) in your browser.
