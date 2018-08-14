package com.skt.tcore.engine

/**
  * Created by kyu hyoung Lee on 2018-08-13.
  */

object TaglessExpr {
  sealed trait Expr[F[_]] {
    def value(v: Double): F[Double]
    def str(v: String): F[String]
    def add(l: F[Double], r: F[Double]): F[Double]
    def concat(l: F[String], r: F[String]): F[String]
  }

  trait Dsl[A] {
    def apply[F[_]](implicit f: Expr[F]): F[A]
  }

  def value(v: Double): Dsl[Double] = new Dsl[Double] {
    override def apply[F[_]](implicit f: Expr[F]): F[Double] = f.value(v)
  }

  def add(l: Dsl[Double], r: Dsl[Double]) = new Dsl[Double] {
    override def apply[F[_]](implicit f: Expr[F]): F[Double] = f.add(l.apply(f), r.apply(f))
  }

  type Id[A] = A
  def interpret: Expr[Id] = new Expr[Id] {
    override def value(v: Double): Id[Double] = v
    override def str(v: String): Id[String] = v
    override def add(l: Id[Double], r: Id[Double]): Id[Double] = l + r
    override def concat(l: Id[String], r: Id[String]): Id[String] = l + r
  }

  final case class Const[A, B](a: A)

  def main(args: Array[String]): Unit = {
    println(
      value(10).apply(interpret)
    )

    println(
      add(value(10), value(20)).apply(interpret)
    )
  }
}
