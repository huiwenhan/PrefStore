package com.ibm.webahead.steer.gizzard.preference

import com.twitter.xrayspecs.Time


case class Preference(
		userId: Long, 
		itemId: Long, 
		score:Double,
		source:String,
		action:String,
		createDate:Int,
		createType:Int
	)