# Common Business Logic

Think Perl AppUtils, grouped by functionality, and expanded to cover
practically all business logic that may be executed via an Editor
object.

Defining logic in shared modules instead of isolating it in chunks
within a service allows applications to execute the full range of needed
logic within a single transaction.

Published OpenSRF APIs are generally thin wrappers around the logic
found here.
