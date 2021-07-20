//
//  SSLTrustingURLSession.swift
//  SSLTrustingURLSession
//
//  Created by Tanmay Bakshi on 2021-07-19.
//

import Foundation

extension Array: Error where Element: Error {}

class SSLTrustingURLSession: NSObject, URLSessionDelegate {
    static var shared = SSLTrustingURLSession()
    
    var session: URLSession!
    
    override init() {
        super.init()
        session = URLSession(configuration: .default, delegate: self, delegateQueue: .current)
    }
    
    func urlSession(_ session: URLSession,
                    didReceive challenge: URLAuthenticationChallenge) async -> (URLSession.AuthChallengeDisposition, URLCredential?) {
        return (.useCredential, URLCredential(trust: challenge.protectionSpace.serverTrust!))
    }
}
