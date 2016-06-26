//
//  Sequence+Extensions.swift
//  ParallelComputing
//
//  Created by Carlos Rodríguez Domínguez on 25/6/16.
//  Copyright © 2016 Everyware Technologies. All rights reserved.
//

import Foundation

extension CollectionType where Self.Index.Distance == Int, Self.Index : Comparable, Self.SubSequence.Generator.Element == Self.Generator.Element {
    public func parallelMap<T>(executor executor:ParallelExecutor, transform: (Self.Generator.Element) throws -> T) -> [T?] {
        let result = ThreadSafeReference([T?](count:self.count, repeatedValue:nil))
        
        for (idx, object) in self.enumerate() {
            executor.addOperationWithBlock{
                let transformedValue = try? transform(object)
                result.performBlock{ $0[idx] = transformedValue }
            }
            
            if idx % executor.maxConcurrentOperationCount == 0 {
                executor.waitUntilAllOperationsAreFinished() //avoid overloading the queue with too many operations
            }
        }
        
        executor.waitUntilAllOperationsAreFinished()
        
        return result.unsafeInternalReference
    }
    
    public func partitionedParallelMap<T>(executor executor:ParallelExecutor, numberOfPartitions:Int, transform: (Self.Generator.Element) throws -> T) rethrows -> [T?] {
        guard numberOfPartitions < self.count && numberOfPartitions > 0 else {
            return try self.map(transform)
        }
        
        let result = ThreadSafeReference([T?](count:self.count, repeatedValue:nil))
        
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
            
            executor.addOperationWithBlock{
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
            
            executor.addOperationWithBlock{
                let subsequence = self[startIndex..<endIndex]
                
                if let partialResult:[T?] = try? subsequence.map(transform) {
                    result.performBlock{ $0.replaceRange(lastEndIndexInt..<self.count, with: partialResult) }
                }
            }
        }
        
        executor.waitUntilAllOperationsAreFinished()
        
        return result.unsafeInternalReference
    }
    
    public func parallelReduce<T>(executor executor:ParallelExecutor, initial:T, combine: (T, Self.Generator.Element) throws -> T) -> T {
        let result = ThreadSafeReference(initial)
        
        for (idx, object) in self.enumerate() {
            executor.addOperationWithBlock{
                result.performBlock{ (currentValue) in
                    if let partialResult = try? combine(currentValue, object) {
                        currentValue = partialResult
                    }
                }
            }
            
            if idx % executor.maxConcurrentOperationCount == 0 {
                executor.waitUntilAllOperationsAreFinished() //avoid overloading the queue with too many operations
            }
        }
        
        executor.waitUntilAllOperationsAreFinished()
        
        return result.unsafeInternalReference
    }
    
    public func partitionedParallelReduce<T>(executor executor:ParallelExecutor, numberOfPartitions:Int, initial:T, combine: (T, Self.Generator.Element) throws -> T) rethrows -> T {
        guard numberOfPartitions < self.count && numberOfPartitions > 0 else {
            return try self.reduce(initial, combine: combine)
        }
        
        let result = ThreadSafeReference(initial)
        
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
            
            executor.addOperationWithBlock{
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
            
            executor.addOperationWithBlock{
                let subsequence = self[startIndex..<endIndex]
                
                result.performBlock{(currentValue) in
                    if let partialResult = try? subsequence.reduce(currentValue, combine: combine) {
                        currentValue = partialResult
                    }
                }
            }
        }
        
        executor.waitUntilAllOperationsAreFinished()
        
        return result.unsafeInternalReference
    }
}
