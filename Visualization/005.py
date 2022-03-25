class A(object):
    @staticmethod
    def P():
        print("Class--A")
class B(object):
    @staticmethod
    def p():
        print("Class--B")
class C:(B,A)
objb = B()
objc = C()
print(objb)
print(objc)
# C.p()         ## 这里为什么报错嘛？报错信息：AttributeError: type object 'C' has no attribute 'p'
B.p()           ## 这里为什么可以不生成一个对象就能调用这个方法嘛？
                ## 为什么这个类没有使用 __init__ 进行构造嘛？
print(C)
import datetime

print(datetime.datetime.now())