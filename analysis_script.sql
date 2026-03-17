with session_info as (
SELECT s.date,
s.ga_session_id,
sp.country,
sp.device,
sp.continent,
sp.channel,
ab.test,
ab.test_group
FROM  `data-analytics-mate.DA.session` s
join  `data-analytics-mate.DA.session_params` as sp
on s.ga_session_id = sp.ga_session_id
join  `data-analytics-mate.DA.ab_test` as ab
on s.ga_session_id = ab.ga_session_id),


session_with_orders as (
select session_info.date,
session_info.country,
session_info.device,
session_info.continent,
session_info.channel,
session_info.test,
session_info.test_group,
count(distinct o.ga_session_id) as orders_cnt
from `data-analytics-mate.DA.order` as o
join session_info
on o.ga_session_id = session_info.ga_session_id
group by session_info.date,
session_info.country,
session_info.device,
session_info.continent,
session_info.channel,
session_info.test,
session_info.test_group),


events as (
SELECT session_info.date,
session_info.country,
session_info.device,
session_info.continent,
session_info.channel,
session_info.test,
session_info.test_group,
ev.event_name,
count(ev.ga_session_id) as event_cnt
FROM  `data-analytics-mate.DA.event_params` as ev
join  session_info
on ev.ga_session_id = session_info.ga_session_id
group by session_info.date,
session_info.country,
session_info.device,
session_info.continent,
session_info.channel,
session_info.test,
session_info.test_group,
ev.event_name),


session as (
select session_info.date,
session_info.country,
session_info.device,
session_info.continent,
session_info.channel,
session_info.test,
session_info.test_group,
count(distinct session_info.ga_session_id) as session_cnt
from session_info
group by session_info.date,
session_info.country,
session_info.device,
session_info.continent,
session_info.channel,
session_info.test,
session_info.test_group),


account as (
select  session_info.date,
session_info.country,
session_info.device,
session_info.continent,
session_info.channel,
session_info.test,
session_info.test_group,
count(distinct acs.ga_session_id) new_account
from `data-analytics-mate.DA.account_session`  as acs
join session_info
on acs.ga_session_id = session_info.ga_session_id
group by session_info.date,
session_info.country,
session_info.device,
session_info.continent,
session_info.channel,
session_info.test,
session_info.test_group)




select session_with_orders.date,
session_with_orders.country,
session_with_orders.device,
session_with_orders.continent,
session_with_orders.channel,
session_with_orders.test,
session_with_orders.test_group,
 'session with orders' as event_name,
session_with_orders.orders_cnt as value
from session_with_orders
union all
select events.date,
events.country,
events.device,
events.continent,
events.channel,
events.test,
events.test_group,
event_name,
event_cnt as value
from events
union all
select session.date,
session.country,
session.device,
session.continent,
session.channel,
session.test,
session.test_group,
'session' as event_name,
session_cnt as value
from session
union all
select account.date,
account.country,
account.device,
account.continent,
account.channel,
account.test,
account.test_group,
'new account' as event_name,
new_account as value
from account
