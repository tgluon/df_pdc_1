用户最大规模为1亿，用户拥有信用评分(信用评分为非负整数，且小于100万)，初始评分不固定而且全部存储在一个乱序的大文件中（每行的内容格式是用户名,信用评分）。
排名规则：信用评分相同，则名次按用户在文件行中出现的先后顺序排序。
（限定内存使用为512MB）
1、给定用户的信用评分名次。
2、Top 100 的信用评分的用户。
3、信用评分重复次数最高的10个信用评分。
