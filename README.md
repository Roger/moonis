Moonis
======

Very basic WIP mini redis implementation to learn [lunatic](lunatic.solutions/).
It is a rewrite of [greenis](https://github.com/Roger/greenis), another mini redis
implementation I did to learn rust and tokio.

Features
--------

* RESP protocol parsing using combine (any redis client can be connected)
* Basic commands: get, set, delete, ping, append, keys, exists, etc
