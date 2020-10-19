# -*- coding: utf-8 -*-
"""
Created on Sat Aug  1 22:31:39 2020

@author: Lorena G
"""
from mrjob.job import MRJob
class MRAverageFriendByAge(MRJob):
    def mapper(self, _, line):
        (userId, name,age,numberFriend) = line.split(',')
        yield age,float(numberFriend)
        
    def reducer(self, age, numberFriend):
        total = 0
        numElements = 0
        for x in numberFriend:
            numElements = numElements+1
            total = total + x
        yield age, total/numElements
        
if __name__ == '__main__':
    MRAverageFriendByAge.run()

        


