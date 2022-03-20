#输入数据
print("输入列表，元素用空格隔开")
lst = input().split(" ")
# print(lst)
lst = [int(i) for i in lst]
print(lst)
#测试部分
print("函数结果：")
def func(lst):
    lst[1] = lst[0] + lst[1]
    lst[0] = lst[1] - lst[0]
    lst[1] = lst[1] - lst[0]
    return lst
print(func(lst))

def win3(l):
    l[1] = l[0] + l[1]
    l[0] = l[1] - l[0]
    l[1] = l[1] - l[0]
    print(l)
res = win3(lst)
print(res)