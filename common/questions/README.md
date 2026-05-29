# HLD Interview Prep — Uber SDE2

Detailed HLDs for the 22 problems covered in [back_of_envelope_estimations_example.md](../back_of_envelope_estimations_example.md). Each file follows the same FAANG-interview structure so you can flip between them quickly during prep.

## Standard Section Order (every file)

1. **Requirements** — Functional + Non-Functional
2. **Scale & Estimates** — short recap (full math lives in the estimations doc)
3. **High-Level Component Design** — LB, API gateway, services, datastores
4. **API Design** — key endpoints with shapes
5. **Data Storage & Schema** — schema, DB choice + justification (why X, why not Y), sharding, replication
6. **Scalability & Performance** — caching, queues, read/write optimization
7. **Deep Dive** — 1–2 focused topics commonly probed by interviewers
8. **Trade-offs** — CAP positioning, consistency model, failure handling
9. **Mermaid Diagram** — final architecture

## Reference Material

- [databases_cheatsheet.md](databases_cheatsheet.md) — what each common database does, when to pick it, when not to, and a decision matrix.

## Problem Index

| # | Problem | File |
|---|---------|------|
| 1 | Logistics Management System | [logistics_management_system.md](logistics_management_system.md) |
| 2 | Stock Price Alerting System | [stock_price_alerting_system.md](stock_price_alerting_system.md) |
| 3 | Order Processing (Top-K Items) | [order_processing_system.md](order_processing_system.md) |
| 4 | Uber Eats — General | [uber_eats_general.md](uber_eats_general.md) |
| 5 | Uber Eats — Restaurant Dashboard | [uber_eats_restaurant_dashboard.md](uber_eats_restaurant_dashboard.md) |
| 6 | Uber Eats — Train PNR Delivery | [uber_eats_train_pnr.md](uber_eats_train_pnr.md) |
| 7 | Uber Eats — Restaurant Feed Backend | [uber_eats_restaurant_feed.md](uber_eats_restaurant_feed.md) |
| 8 | Uber Driver Location / Heatmap | [uber_driver_location_heatmap.md](uber_driver_location_heatmap.md) |
| 9 | Meeting Scheduler (N rooms) | [meeting_scheduler.md](meeting_scheduler.md) |
| 10 | Simplified Twitter | [simplified_twitter.md](simplified_twitter.md) |
| 11 | Product Browsing (E-commerce) | [product_browsing_ecommerce.md](product_browsing_ecommerce.md) |
| 12 | Omegle-like Match + Chat | [omegle_match_chat.md](omegle_match_chat.md) |
| 13 | Multiplayer Online Chess | [multiplayer_chess.md](multiplayer_chess.md) |
| 14 | Distributed Notification Service | [distributed_notification_service.md](distributed_notification_service.md) |
| 15 | Proximity Service (Yelp-like) | [proximity_service.md](proximity_service.md) |
| 16 | Event Processor (Cart/View events) | [event_processor.md](event_processor.md) |
| 17 | Hotel Booking System | [hotel_booking_system.md](hotel_booking_system.md) |
| 18 | Logging Application | [logging_application.md](logging_application.md) |
| 19 | Billing Aggregation API | [billing_aggregation_api.md](billing_aggregation_api.md) |
| 20 | Trending Items Backend | [trending_items_backend.md](trending_items_backend.md) |
| 21 | Photo Upload + Geo-tag | [photo_upload_geotag.md](photo_upload_geotag.md) |
| 22 | YouTube View Count | [youtube_view_count.md](youtube_view_count.md) |
| 23 | Food Delivery App (DoorDash / Swiggy style) | [food_delivery_app.md](food_delivery_app.md) |

## How to Use Before an Interview

- Skim the **Requirements** + **Mermaid** + **Trade-offs** for any problem in ~3 minutes.
- For deeper review, read the **DB Choice** justification — that's where most "why not X" interviewer questions land.
- Cross-reference numbers with the [estimations doc](../back_of_envelope_estimations_example.md).
