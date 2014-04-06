#_*_coding:utf-8 _*_
#!/bin/env python
import copy

def FilterGroupLimit(data, idx_k, idx_v, get_v_idx = (-1,None)):  
    '''
    #idx_k = {0:(0,10),1:(None,None)}  
    #get_v_idx = int
    #按照一个字段分组，然后每组数据取其中某字段值最大的那条数据  idx_k 是分组字段的index   idx_v是最大值字段的index   (0:10)是对key数据的截取
    #def FilterGroupLimit(data,idx_k,idx_v,get_v_idx = 1,desc = True):  #idx_k = {0:(0,10),1:(None,None)}  #get_v_idx = int
    '''
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
            

def FilterList(data,idx_v,flist):
    '''
    按照某字段根据提供的列表进行过滤，只返回列表中存在的数据
    '''
    rst = []
    for l in data:
        if l[idx_v] in flist:
            rst.append(l)
    return rst
    

def FilterNoise(data, threshold): #need pageby, 
    '''
        对data数据进行处理：目标是按照第二列数据进行分组，每一组中的最后一列字段的最大值若小于整体最大值的百分之threshold，则清理掉该组所有数据。
        常用在绘制线图的时候，按照某个纬度画多条线，清理掉数值偏小的垃圾数据
    '''
    threshold = float(threshold)
    max_v = 0
    max_e = {}
    noisedata = []
    newdata = []
    for i in data:
        if i[-1] > max_v : max_v = i[-1]
        if max_e.get(i[1],''):
            if i[-1] > max_e[i[1]]:
                max_e[i[1]] = i[-1]
        else:
            max_e[i[1]] = i[-1]
    if max_v == 0:
        return data
    for i in max_e:
        v = max_e[i] or 0
        if 100*(float(v)/float(max_v))<threshold:
            noisedata.append(i)
    for i in data:
        if i[1] not in noisedata:
            newdata.append(i)
    return newdata

def FillGroupbyWithTime(data):
    '''
        对data数据进行处理；目标是对第二列的每一个不同的值按照时间点不全（某个时间点缺失的话用0补）
    '''
    pagebys = {}.fromkeys([i[1] for i in data]).keys()
    l = len(data[0])
    thistime = data[0][0]
    thispageby = []
    adddata = []
    for i in data+[['0','x']]:
        if thistime == i[0]:
            thispageby.append(i[1])
        else:
            x = copy.copy(pagebys)
            for p in thispageby:
                try:
                    x.remove(p)
                except ValueError:
                    pass
            for p in x:
                adddata.append(tuple([thistime,p]+[0 for j in range(l-2)]))
            thispageby = [i[1],]
            thistime = i[0]
                
    newdata = data+adddata
    newdata.sort()
    return newdata

        
if __name__ == '__main__':
    data = [['28','a',10],('28','b',2),('29','a',13),('30','b',11)]
    FilterGroupLimit(data, {0:(None,None)},  1, (-1,None))
    FilterGroupLimit(data, {0:(None,None)},  1, (-1,None))
    FilterGroupLimit(data, {0:(None,None)},  -1, (-1,None))
    FilterGroupLimit(data, {1:(None,None)},  2, (-1,None))
