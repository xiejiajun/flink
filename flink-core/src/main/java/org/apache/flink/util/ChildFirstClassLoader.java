/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * A variant of the URLClassLoader that first loads from the URLs and only after that from the parent.
 *
 * <p>{@link #getResourceAsStream(String)} uses {@link #getResource(String)} internally so we
 * don't override that.
 */
public final class ChildFirstClassLoader extends FlinkUserCodeClassLoader {

	/**
	 * The classes that should always go through the parent ClassLoader. This is relevant
	 * for Flink classes, for example, to avoid loading Flink classes that cross the
	 * user-code/system-code barrier in the user-code ClassLoader.
	 */
	private final String[] alwaysParentFirstPatterns;

	public ChildFirstClassLoader(
			URL[] urls,
			ClassLoader parent,
			String[] alwaysParentFirstPatterns,
			Consumer<Throwable> classLoadingExceptionHandler) {
		super(urls, parent, classLoadingExceptionHandler);
		this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
	}

	/**
	 * 重写FlinkUserCodeClassLoader的loadClassWithoutExceptionHandling，优先加载
	 * @param name
	 * @param resolve
	 * @return
	 * @throws ClassNotFoundException
	 */
	@Override
	protected synchronized Class<?> loadClassWithoutExceptionHandling(
			String name,
			boolean resolve) throws ClassNotFoundException {

		// First, check if the class has already been loaded
		Class<?> c = findLoadedClass(name);

		if (c == null) {
			// check whether the class should go parent-first
			for (String alwaysParentFirstPattern : alwaysParentFirstPatterns) {
				if (name.startsWith(alwaysParentFirstPattern)) {
					// TODO 需要从parent类加载器加载的就走FlinkUserCodeClassLoader的加载方法
					return super.loadClassWithoutExceptionHandling(name, resolve);
				}
			}

			try {
				// check the URLs
				// TODO 优先从自己构建自定义ClassLoader时传入的URL中查找并加载class
				c = findClass(name);
			} catch (ClassNotFoundException e) {
				// let URLClassLoader do it, which will eventually call the parent
				// TODO 自己加载不了再走双亲委托机制
				c = super.loadClassWithoutExceptionHandling(name, resolve);
			}
		}

		if (resolve) {
			resolveClass(c);
		}

		return c;
	}

	/**
	 * TODO 通过重写getResource修改从ClassLoader中查找其他配置文件(如xml文件等)的查找顺序
	 *    优先从自己的ucp中找，找不到再走双亲委托机制从父加载器开始找
	 *    需要注意的是这个方法和class加载没多大关系，注意用来加载ClassLoader的ucp(URLClassPath)中的配置文件
	 * @param name
	 * @return
	 */
	@Override
	public URL getResource(String name) {
		// first, try and find it via the URLClassloader
		URL urlClassLoaderResource = findResource(name);

		if (urlClassLoaderResource != null) {
			return urlClassLoaderResource;
		}

		// delegate to super
		return super.getResource(name);
	}

	/**
	 * TODO 通过重写getResource修改从ClassLoader中查找其他配置文件(如xml文件等)的查找顺序
	 * 	 优先从自己的ucp中找，找不到再走双亲委托机制从父加载器开始找
	 * 	 需要注意的是这个方法和class加载没多大关系，注意用来加载ClassLoader的ucp(URLClassPath)中的配置文件
	 * @param name
	 * @return
	 * @throws IOException
	 */
	@Override
	public Enumeration<URL> getResources(String name) throws IOException {
		// first get resources from URLClassloader
		Enumeration<URL> urlClassLoaderResources = findResources(name);

		final List<URL> result = new ArrayList<>();

		while (urlClassLoaderResources.hasMoreElements()) {
			result.add(urlClassLoaderResources.nextElement());
		}

		// get parent urls
		Enumeration<URL> parentResources = getParent().getResources(name);

		while (parentResources.hasMoreElements()) {
			result.add(parentResources.nextElement());
		}

		return new Enumeration<URL>() {
			Iterator<URL> iter = result.iterator();

			public boolean hasMoreElements() {
				return iter.hasNext();
			}

			public URL nextElement() {
				return iter.next();
			}
		};
	}
}
