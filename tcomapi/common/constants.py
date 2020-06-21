CSV_SEP = ';'
CSV_SEP_REPLACE = ' '

SERVER_IS_DOWN = 'Address is unreachable. Exiting...'


PROLOGUE = """
There could be parsed BINs.
It may take some time. Building list ..."""

KGD_STATUS_EXPLANATION = """
> rqe - count of BINs we got KGD API error with. Those BINs are not supposed to be reprocessed.
> rse - count of occurrences when we got bad response(html(from proxy e.g. squid) or some 
other trash not xml formatted). Those BINs are supposed to be reprocessed.
> se - count of occurrences when we failed(connection, network, 500, etc ).
Those BINs are supposed to be reprocessed. 
> s - count of BINs we successfully processed
> R - indicates reprocessing
>> All these indicators are actual for only this launch! 
"""