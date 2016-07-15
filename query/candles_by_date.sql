select * from
  [{{ task.rdataset }}.{{ task.rtable }}]
where
  date(time) >= '{{ task.sdate.strftime( "%Y-%m-%d" ) }}'
  and date(time) <= '{{ task.edate.strftime( "%Y-%m-%d" ) }}'
order by
   time
  ,code
;
