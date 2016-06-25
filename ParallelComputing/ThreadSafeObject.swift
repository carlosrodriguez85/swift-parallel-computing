//
//  DistributedObject.swift
//  ParallelComputing
//
//  Created by Carlos Rodríguez Domínguez on 25/6/16.
//  Copyright © 2016 Everyware Technologies. All rights reserved.
//

import Foundation

class ThreadSafeReference<T> {
    private var internalObject:T
    private let lockQueue = dispatch_queue_create("es.everywaretech.LockQueue", nil)
    private var lazyOperations:[()->Void] = []
    
    var unsafeInternalReference:T {
        return internalObject
    }
    
    init(_ object:T) {
        self.internalObject = object
    }
    
    func performBlock(block:(inout T)->Void) {
        dispatch_sync(lockQueue) {
            block(&self.internalObject)
        }
    }
}
