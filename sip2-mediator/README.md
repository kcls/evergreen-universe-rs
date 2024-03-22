# sip2-mediator-rs

Rust SIP2 &lt;=> HTTP &lt;=> SIP Mediator

SEE: [https://github.com/berick/SIP2Mediator](https://github.com/berick/SIP2Mediator)

## Setup and Usage (Ubuntu 22.04)

> **_NOTE:_** The mediator leverages some Rust evergreen utilities.  Building the mediator will also fetch/build deps for evergreen/opensrf.  These may eventually be decoupled.


```sh
git clone https://github.com/kcls/evergreen-universe-rs                                  
cd evergreen-universe-rs
make build-sip2mediator-release                                                
sudo make install-sip2mediator-release                                         

sudo cp /usr/local/etc/eg-sip2-mediator.example.yml /usr/local/etc/eg-sip2-mediator.yml
# Edit as needed: /usr/local/etc/eg-sip2-mediator.yml

sudo systemctl start eg-sip2-mediator
```
