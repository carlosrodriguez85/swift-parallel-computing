//
//  ParallelExecutor.swift
//  ParallelComputing
//
//  Created by Carlos Rodríguez Domínguez on 26/6/16.
//  Copyright © 2016 Everyware Technologies. All rights reserved.
//

import Foundation

public protocol ParallelExecutor {
    var maxConcurrentOperationCount:Int{get set}
    
    func addOperationWithBlock(block:()->Void)
    func waitUntilAllOperationsAreFinished()
    func cancelAllOperations()
}

extension NSOperationQueue : ParallelExecutor {
    
}