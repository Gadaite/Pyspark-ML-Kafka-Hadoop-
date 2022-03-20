a=[12,20,60,30,40,70,12,80]
b=[]
for x in a:
    if x<60:
        b.append(x)
for i in b:
    a.remove(i)
print(a)
print(id(a))


win3 = "hello"