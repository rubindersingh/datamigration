package com.migrate

import migration.Progress
import migration.ProgressIndex
import org.quartz.InterruptableJob
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
import org.quartz.UnableToInterruptJobException

class MigrateJob {
    ProductCategoryService productCategoryService
    UserService userService
    NewsLetterService newsLetterService
    ProductService productService
    CouponService couponService
    DiscountService discountService
    ReviewService reviewService
    TestimonialService testimonialService
    CartService cartService
    OrderService orderService

    def group = "MigrateJobGroup"
    static volatile def isInterrupted = false
    static volatile def isRunning = false

    def execute(def params) {
        try {
            isRunning = true
            log.info "-----------------Starting job---------------------------"
            Long progressId = params.mergedJobDataMap.get("id")
            //Preparing meta data for migration
            CommonService.prepareMasterData()

            def val = 'CATEGORY'
            switch (val) {
                case 'CATEGORY':
                    CommonUtils.callTiming({
                        productCategoryService.migrate(progressId)
                    }, "Total Time For Category Migration:")
                case 'PRODUCT':
                    CommonUtils.callTiming({
                        productService.migrate(progressId)
                    }, "Total Time For Product Migration:")
                case 'NEWSLETTER':
                    CommonUtils.callTiming({
                        newsLetterService.migrate(progressId)
                    }, "Total Time For Newsletter Migration:")
                case 'USER':
                    CommonUtils.callTiming({
                       userService.migrate(progressId)
                    }, "Total Time For User Migration:")
                case 'TESTIMONIAL':
                    CommonUtils.callTiming({
                        testimonialService.migrate(progressId)
                    }, "Total Time For Testimonial Migration:")
                case 'REVIEW':
                    CommonUtils.callTiming({
                        reviewService.migrate(progressId)
                    }, "Total Time For Review Migration:")
                case 'COUPON':
                    CommonUtils.callTiming({
                        couponService.migrate(progressId)
                    }, "Total Time For Coupon Migration:")
                case 'ORDER':
                    CommonUtils.callTiming({
                        orderService.migrate(progressId)
                    }, "Total Time For Order Migration:")
            }

        } catch (Exception e) {
            log.error(e.message,e)
        }
        finally {
            isRunning = false
        }
    }
}
