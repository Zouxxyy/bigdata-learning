/************************基本查询****************************/

# 建表
create table if not exists emp(empno int, ename string, job string, mgr int,
hiredate string, sal double, comm double, deptno int)
row format delimited fields terminated by '\t';

create table if not exists dept(deptno int, dname string, loc int)
row format delimited fields terminated by '\t';

# 导入数据
load data local inpath '/Users/zxy/IdeaProjects/bigdata-learning/hive-learning/data/input/emp.txt' into table emp;

load data local inpath '/Users/zxy/IdeaProjects/bigdata-learning/hive-learning/data/input/dept.txt' into table dept;

# 简单查询
select empno, ename from emp;

select count(*) cnt from emp;

select * from emp where sal >1000 limit 5;

# group by测试：计算emp表每个部门的平均工资
select t.deptno, avg(t.sal) avg_sal from emp t group by t.deptno;

# having测试：求每个部门的平均薪水大于2000的部门
select deptno, avg(sal) avg_sal from emp group by deptno having avg_sal > 2000;

# join测试：合并员工表和部门表
select * from emp e join dept d on e.deptno = d.deptno;


/************************排序****************************/

# 查询员工信息按工资降序排列
select * from emp order by sal desc;

# 分组排序：设置reduce个数
set mapreduce.job.reduces=3;
# 先按照部门编号分区，再按照员工编号降序排序
insert overwrite local directory '/Users/zxy/IdeaProjects/bigdata-learning/hive-learning/data/output'
select * from emp distribute by deptno sort by empno desc;

# 分桶，以下两种写法等价
select * from emp cluster by deptno;
select * from emp distribute by deptno sort by deptno;


/************************行转列****************************/

# 建表
create table constellation (name string, constellation string, blood_type string)
row format delimited fields terminated by "\t";

# 导入数据
load data local inpath '/Users/zxy/IdeaProjects/bigdata-learning/hive-learning/data/input/constellation.txt'
into table constellation;

# 把星座和血型一样的人归类到一起

# 1. 连接字段
select
    name,
    concat(constellation, ",", blood_type) base
from
    constellation; t1

# 2. 行转列
select
    base,
    concat_ws("|", collect_set(name)) name
from
(select
    name,
    concat(constellation, ",", blood_type) base
from
    constellation) t1
group by
    base;

/************************列转行****************************/

# 建表
create table movie_info (movie string, category array<string>)
row format delimited fields terminated by "\t"
collection items terminated by ",";

# 导入数据
load data local inpath '/Users/zxy/IdeaProjects/bigdata-learning/hive-learning/data/input/movie.txt'
into table movie_info;

# 将电影分类中的数组数据展开
select
    movie,
    category_name
from 
    movie_info lateral view explode(category) table_tmp as category_name;


/************************窗口函数****************************/

# 建表
create table business (name string, orderdate string, cost int)
row format delimited fields terminated by ",";

# 导入数据
load data local inpath '/Users/zxy/IdeaProjects/bigdata-learning/hive-learning/data/input/business.txt'
into table business;

#（1）查询在2017年4月份购买过的顾客及总人数，over()针对groupby后的全部数据
select
    name,
    count(*) over () 
from business 
where 
    substring(orderdate,1,7) = '2017-04' 
group by 
    name;

#（2）查询顾客的购买明细 及 cost按照日期进行累加
select
    name,
    orderdate,
    cost,
    sum(cost) over(partition by name order by orderdate) 
from
    business;

#（3）查看顾客上次的购买时间，在窗口中分区排序
select
    name,
    orderdate,
    cost, 
    lag(orderdate, 1, '1970-01-01') over(partition by name order by orderdate)
from business;

#（4）查询前20%时间的订单信息

# 加分组号
select
    name,
    orderdate,
    cost, 
    ntile(5) over(order by orderdate) sorted
from business; t1

# 过滤出组号为1的数据
select
    name,
    orderdate,
    cost
from 
(select
    name,
    orderdate,
    cost, 
    ntile(5) over(order by orderdate) sorted
from business) t1
where 
    sorted = 1;

#（5）计算每个人消费的排名
select
    name,
    orderdate,
    cost,
    rank() over(partition by name order by cost desc)
from
    business;
