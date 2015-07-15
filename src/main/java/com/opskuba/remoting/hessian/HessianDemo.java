package com.opskuba.remoting.hessian;

import java.net.MalformedURLException;

import com.caucho.hessian.client.HessianProxyFactory;
import com.caucho.hessian.io.HessianRemoteObject;

public class HessianDemo {

	public static void main(String[] args) throws MalformedURLException {
		String url = "http://hessian.caucho.com/test/test";

		HessianProxyFactory factory = new HessianProxyFactory();
		HessianRemoteObject basic = (HessianRemoteObject) factory.create(
				TestAPI.class, url);

		System.out.println(basic.getHessianType());
		System.out.println(basic.getHessianURL());

	}

}
