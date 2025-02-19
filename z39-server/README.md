# Evergreen Z39.50 Server

## TODO
* inactivity timeout
* Holdings:
  * different databases, e.g. one that returns holdings.
  * https://github.com/kcls/evergreen-pub/commit/5653a29782dd6e07f833a35c385c7d3cd64423e8
  * See eg-marc-export
  * any kind of config/templating for marc holdings fields?

```
ush @copies, {                                                                
    a => $copy->getChildrenByTagName('location')->[0]->textContent,            
    b => $owning_lib,                                                          
    c => $cn,                                                                  
    d => $copy->getChildrenByTagName('circ_lib')->[0]->getAttribute('shortname'),
    g => $copy->getAttribute('barcode'),                                       
    k => $prefix,                                                              
    m => $suffix,                                                              
    n => $copy->getChildrenByTagName('status')->[0]->textContent,              
    q => $copy->{circ_lib_name},                                               
    r => $copy->{acpl_name},                                                   
}; 

```


