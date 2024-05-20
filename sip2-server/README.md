# Evergreen SIP2 Server in Rust

> **_NOTE:_** This code may soon be deprecated since Evergreen now 
officially supports [SIP2 Mediator](../sip2-mediator/README.md).  Parts of 
this code, however, may later be used in a Rust implementation of the 
sip2-mediator backend.

## Supported Message Pairs

* 99/98 ACS/SC Status
* 93/94 Login
* 09/10 Checkin
* 11/12 Checkout
* 17/18 Item Information
* 23/24 Patron Status
* 35/36 End Patron Session (No-Op)
* 37/38 Fee Paid
* 63/64 Patron Information

## Future Development

* 29/30 Renew Message
* 65/66 Renew All Message
* 01/24 Block Patron Message
* 97/\* Request ACS Resend Message
* Actor/Asset Stat Cat SIP Fields Support




