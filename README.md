SneakerShop — Microservices E-Commerce System

SneakerShop is a microservices-based e-commerce application for sneaker sales that implements asynchronous communication using RabbitMQ.
This project was developed as a demonstration of modern distributed system architecture using Docker and Flask-based services.

Features
1. Microservices Architecture
2. API Gateway
3. Asynchronous Communication with RabbitMQ
4. Docker Containerization
5. Separate Database for Each Service
6. REST API Communication
7. Interactive Frontend Dashboard

Microservices Flow
1. User accesses the frontend application.
2. Request is sent through API Gateway.
3. API Gateway routes the request to the appropriate service.
4. Order Service sends order event to RabbitMQ queue.
5. Payment Service consumes the queue asynchronously.
6. Payment status is updated without blocking the main application flow.

| Member | Responsibility  |
| ------ | --------------- |
| Mutiara Adzika   | User Service    |
| Rayindarari Damba  | Product Service |
| Masyta Gita Faradilla   | Order Service   |
| Alma Amanina Mujahida   | Payment Service |
