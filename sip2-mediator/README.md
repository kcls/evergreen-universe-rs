# sip2-mediator-rs

Rust SIP2 Mediator Direct Evergreen API Interface

## About

This is a vairiant of the SIP2 <-> HTTP mediator for Evergreen which 
bypassses the HTTP layer, opting instead to communicate directly
with Evergreen via its API.

### See 

* [https://bugs.launchpad.net/evergreen/+bug/1901930](Evergreen Bug #1901930)
    * This branch must be running on your Evergreen server.
* [https://github.com/berick/SIP2Mediator](https://github.com/berick/SIP2Mediator)
    * The code in this repository effectively replaces the code from the above.

## The Basics (Ubuntu 22.04)

```sh
git clone https://github.com/kcls/evergreen-universe-rs                                  
cd evergreen-universe-rs
make build-sip2mediator-release                                                
sudo make install-sip2mediator-release                                         

# Edit as needed: /usr/local/etc/eg-sip2-mediator.yml

sudo systemctl start eg-sip2-mediator
```
