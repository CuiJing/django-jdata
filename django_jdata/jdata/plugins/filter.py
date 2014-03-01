#_*_coding:utf-8 _*_
#!/bin/env python

#按照一个字段分组，然后每组数据取其中某字段值最大的那条数据  idx_k 是分组字段的index   idx_v是最大值字段的index   (0:10)是对key数据的截取
#def FilterGroupLimit(data,idx_k,idx_v,get_v_idx = 1,desc = True):  #idx_k = {0:(0,10),1:(None,None)}  #get_v_idx = int
def FilterGroupLimit(data,idx_k,idx_v,get_v_idx = (-1,None)):  #idx_k = {0:(0,10),1:(None,None)}  #get_v_idx = int
    rst = {}
    for l in data:
        k = tuple([ l[j][idx_k[j][0]:idx_k[j][1]] for j in idx_k])   #k是分组的key，支持多个字段，取每个字段截取后的值
        v = l[idx_v]
        if not k in rst.keys():
            rst[k] = [(v,l),]
        else:
            rst[k].append((v,l))
    result = []
    for k,p in rst.items():
        p.sort()
        d = p[get_v_idx[0]:get_v_idx[1]]
        for i in d:
            result.append(i[1])
    result.sort()
    return result
            

#按照某字段根据提供的列表进行过滤，只返回列表中存在的数据
def FilterList(data,idx_v,flist):
    rst = []
    for l in data:
        if l[idx_v] in flist:
            rst.append(l)
    return rst
    
if __name__ == '__main__':

    data = [['28','a',10],('28','b',2),('29','a',13),('30','b',11)]
    FilterGroupLimit(data, {0:(None,None)},  1, (-1,None))
    FilterGroupLimit(data, {0:(None,None)},  1, (-1,None))
    FilterGroupLimit(data, {0:(None,None)},  -1, (-1,None))
    FilterGroupLimit(data, {1:(None,None)},  2, (-1,None))

