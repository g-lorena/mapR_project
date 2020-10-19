# -*- coding: utf-8 -*-
"""
Created on Sun Aug  2 18:46:53 2020

@author: Lorena G
"""
from mrjob.job import MRJob

class MRSpentByCustomer(MRJob):
    def mapper(self, _, line):
        (customerId,item,orderAmount) = line.split(',')
        yield customerId,float(orderAmount)
        
    def reducer(self, customerId, orderAmount):
        yield customerId, sum(orderAmount)
        
if __name__ == '__main__':
    MRSpentByCustomer.run() 