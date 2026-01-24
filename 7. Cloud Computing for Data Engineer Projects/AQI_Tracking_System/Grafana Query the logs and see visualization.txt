Query to see your records:

fields @timestamp, @message
| parse @message /"window_end":\s*"(?<window_end>[^"]+)",\s*"record_count":\s*(?<record_count>\d+)/
| display window_end, record_count
| sort @timestamp desc
| limit 20



Basic Grafana Query 
fields @timestamp, @message
| limit 20



Max and Min Logic Query
fields @timestamp, @message
| parse @message /"record_count":\s*(?<record_count>\d+)/
| stats max(record_count) as max_rc, min(record_count) as min_rc, avg(record_count) as avg_rc by bin(5m)
