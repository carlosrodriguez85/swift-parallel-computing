//
//  Sequence+Extensions.swift
//  ParallelComputing
//
//  Created by Carlos Rodríguez Domínguez on 25/6/16.
//  Copyright © 2016 Everyware Technologies. All rights reserved.
//

import Foundation

extension CollectionType where Self.Index.Distance == Int, Self.Index : Comparable, Self.SubSequence.Generator.Element == Self.Generator.Element {
    public func parallelMap<T>(maxConcurrentOperations maxConcurrentOperations:Int, transform: (Self.Generator.Element) throws -> T) -> [T?] {
        let result = ThreadSafeReference([T?](count:self.count, repeatedValue:nil))
        let operationQueue = NSOperationQueue()
        operationQueue.qualityOfService = .UserInitiated
        operationQueue.maxConcurrentOperationCount = maxConcurrentOperations
        
        for (idx, object) in self.enumerate() {
            operationQueue.addOperationWithBlock{
                let transformedValue = try? transform(object)
                result.performBlock{ $0[idx] = transformedValue }
            }
            
            if idx % maxConcurrentOperations == 0 {
                operationQueue.waitUntilAllOperationsAreFinished() //avoid overloading the queue with too many operations
            }
        }
        
        operationQueue.waitUntilAllOperationsAreFinished()
        
        return result.unsafeInternalReference
    }
    
    public func partitionedParallelMap<T>(numberOfPartitions numberOfPartitions:Int, transform: (Self.Generator.Element) throws -> T) rethrows -> [T?] {
        guard numberOfPartitions < self.count && numberOfPartitions > 0 else {
            return try self.map(transform)
        }
        
        let result = ThreadSafeReference([T?](count:self.count, repeatedValue:nil))
        let operationQueue = NSOperationQueue()
        operationQueue.qualityOfService = .UserInitiated
        operationQueue.maxConcurrentOperationCount = numberOfPartitions
        
        let partitionSize:Int = self.count / numberOfPartitions
        
        var lastEndIndexInt = 0
        for partitionNumber in 0..<numberOfPartitions {
            let startIndexInt = partitionNumber*partitionSize
            var endIndexInt = startIndexInt+partitionSize
            
            let startIndex = self.startIndex.advancedBy(startIndexInt)
            var endIndex = startIndex.advancedBy(partitionSize)
            if self.endIndex < endIndex {
                endIndex = self.endIndex
                endIndexInt = self.count
            }
            
            lastEndIndexInt = endIndexInt
            
            operationQueue.addOperationWithBlock{
                let subsequence = self[startIndex..<endIndex]
                
                if let partialResult:[T?] = try? subsequence.map(transform) {
                    result.performBlock{ $0.replaceRange(startIndexInt..<endIndexInt, with: partialResult) }
                }
            }
        }
        
        //map remaining objects
        if lastEndIndexInt < self.count {
            let startIndex = self.startIndex.advancedBy(lastEndIndexInt)
            let endIndex = self.endIndex
            
            operationQueue.addOperationWithBlock{
                let subsequence = self[startIndex..<endIndex]
                
                if let partialResult:[T?] = try? subsequence.map(transform) {
                    result.performBlock{ $0.replaceRange(lastEndIndexInt..<self.count, with: partialResult) }
                }
            }
        }
        
        operationQueue.waitUntilAllOperationsAreFinished()
        
        return result.unsafeInternalReference
    }
    
    public func parallelReduce<T>(maxConcurrentOperations maxConcurrentOperations:Int, initial:T, combine: (T, Self.Generator.Element) throws -> T) -> T {
        let result = ThreadSafeReference(initial)
        let operationQueue = NSOperationQueue()
        operationQueue.qualityOfService = .UserInitiated
        operationQueue.maxConcurrentOperationCount = maxConcurrentOperations
        
        for (idx, object) in self.enumerate() {
            operationQueue.addOperationWithBlock{
                result.performBlock{ (currentValue) in
                    if let partialResult = try? combine(currentValue, object) {
                        currentValue = partialResult
                    }
                }
            }
            
            if idx % maxConcurrentOperations == 0 {
                operationQueue.waitUntilAllOperationsAreFinished() //avoid overloading the queue with too many operations
            }
        }
        
        operationQueue.waitUntilAllOperationsAreFinished()
        
        return result.unsafeInternalReference
    }
    
    public func partitionedParallelReduce<T>(numberOfPartitions numberOfPartitions:Int, initial:T, combine: (T, Self.Generator.Element) throws -> T) rethrows -> T {
        guard numberOfPartitions < self.count && numberOfPartitions > 0 else {
            return try self.reduce(initial, combine: combine)
        }
        
        let result = ThreadSafeReference(initial)
        let operationQueue = NSOperationQueue()
        operationQueue.qualityOfService = .UserInitiated
        operationQueue.maxConcurrentOperationCount = numberOfPartitions
        
        let partitionSize:Int = self.count / numberOfPartitions
        
        var lastEndIndexInt = 0
        for partitionNumber in 0..<numberOfPartitions {
            let startIndexInt = partitionNumber*partitionSize
            var endIndexInt = startIndexInt+partitionSize
            
            let startIndex = self.startIndex.advancedBy(startIndexInt)
            var endIndex = startIndex.advancedBy(partitionSize)
            if self.endIndex < endIndex {
                endIndex = self.endIndex
                endIndexInt = self.count
            }
            
            lastEndIndexInt = endIndexInt
            
            operationQueue.addOperationWithBlock{
                let subsequence = self[startIndex..<endIndex]
                
                result.performBlock{(currentValue) in
                    if let partialResult = try? subsequence.reduce(currentValue, combine: combine) {
                        currentValue = partialResult
                    }
                }
            }
        }
        
        //map remaining objects
        if lastEndIndexInt < self.count {
            let startIndex = self.startIndex.advancedBy(lastEndIndexInt)
            let endIndex = self.endIndex
            
            operationQueue.addOperationWithBlock{
                let subsequence = self[startIndex..<endIndex]
                
                result.performBlock{(currentValue) in
                    if let partialResult = try? subsequence.reduce(currentValue, combine: combine) {
                        currentValue = partialResult
                    }
                }
            }
        }
        
        operationQueue.waitUntilAllOperationsAreFinished()
        
        return result.unsafeInternalReference
    }
}
