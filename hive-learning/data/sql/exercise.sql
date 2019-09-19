/*************************练习一****************************/

# 建表
create table visit (userId string, visitDate string, visitCount int)
row format delimited fields terminated by "\t";

# 导入数据
load data local inpath '/Users/zxy/IdeaProjects/bigdata-learning/hive-learning/data/input/visit.txt'
into table visit;

# 统计每个用户累计的访问次数

# 1. 格式化
select 
    userId,
    date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM') mn,
    visitCount
from 
    visit; t1

# 2. group by userId, mn 得到用户按月小计
select
    userId,
    mn,
    sum(visitCount) sum1
from
(select 
    userId,
    date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM') mn,
    visitCount
from 
    visit) t1
group by
    userId, mn; t2

# 3. 使用窗口函数
select
    userId,
    mn,
    sum1,
    sum(sum1) over (partition by userId order by mn)
from
(select
    userId,
    mn,
    sum(visitCount) sum1
from
(select 
    userId,
    date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM') mn,
    visitCount
from 
    visit) t1
group by
    userId, mn) t2;



/*************************练习二****************************/

# 建表
create table jd (user_id string, shop string)
row format delimited fields terminated by "\t";

# 导入数据
load data local inpath '/Users/zxy/IdeaProjects/bigdata-learning/hive-learning/data/input/jd.txt'
into table jd;

# 统计每个店铺的UV（访客数）

# 1. 去重 （尽量别用distinct）
select
    shop,
    user_id
from
    jd
group by 
    shop, user_id; t1

# 2. group by 计数
select
    shop,
    count(user_id) uv
from
(select
    shop,
    user_id
from
    jd
group by 
    shop, user_id) t1
group by 
    shop;


# 统计每个店铺访问的top3信息：店铺名称、访问id、访问次数 (分组topn)

# 1. group by shop, user_id
select
    shop,
    user_id,
    count(*) ct
from
    jd
group by
    shop, user_id; t1

# 2. rank
select
    shop,
    user_id,
    ct,
    row_number() over(partition by shop order by ct desc) rk
from
(select
    shop,
    user_id,
    count(*) ct
from
    jd
group by
    shop, user_id)t1; t2

# 3. 取前3名
select
    shop,
    user_id,
    ct
from
(select
    shop,
    user_id,
    ct,
    row_number() over(partition by shop order by ct desc) rk
from
(select
    shop,
    user_id,
    count(*) ct
from
    jd
group by
    shop, user_id)t1) t2
where
    rk <= 3;

/*************************练习三****************************/

# 蚂蚁森林的数据太多了，造起来麻烦。跳过了吧，可以专门找书刷sql题，别执着一棵树上。准备以后再开个仓库专门学sql。

