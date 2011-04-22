/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.davidgreco.graphbase.dsl

class element[A, T <: {
  def setProperty(key : String, value : AnyRef) : Unit
  def getProperty(key : String) : AnyRef
  def removeProperty(key : String) : AnyRef
  def getId() : AnyRef
}](e: T) {

  def getId: AnyRef = e.getId

  def setProperty[P](prop: Tuple2[String, P]): A = {
    e.setProperty(prop._1, prop._2.asInstanceOf[AnyRef])
    this.asInstanceOf[A]
  }

  def getProperty(key: String): Option[AnyRef] = {
    Option(e.getProperty(key))
  }

  def removeProperty(key: String): Option[AnyRef] = {
    Option(e.removeProperty(key))
  }

  def <=[P](prop: Tuple2[String, P]) = {
    this.setProperty(prop)
  }

  def >=(key: String) = {
    this.getProperty(key)
  }

  def -=(key: String) = {
    this.removeProperty(key)
  }

 def unary_~ : AnyRef = this.getId

}