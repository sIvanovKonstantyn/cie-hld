# Payment processing system design

## Overview
_System processing payments. 
Takes care about durable and fast payment processing. 
Service built to replace a legacy payment processing component._

## Functional requirements
Service should support a payment processing feature which contains next steps:
- Payment creation
- Bank verification of payment
- Financial monitoring of payment
- Tracing all the payment statuses changes and errors that may occur during the process

## Non-functional requirements
As a part of requirements service should support next non-functional features:
- **performance:** the latency of the service should be 1000ms (at least 50% smaller than in legacy systems). We should be notified about all the cases that cross this threshold.
- **durability:** payment process should be highly durable. Each payment status should be a save point from where the system should be able to continue the process. All the errors and unusual cases are reported.


## Solution

The service is called by payment processing web application, 
makes calls to bank data system, financial monitoring system, 
kafka message broker and its own database...
```mermaid
C4Context
      title System Context diagram for Payment processing system
      Person(BankAnalyst, "Bank analyst", "Bank employee responsible for payment analyses")
      Rel(BankAnalyst, PaymentReportingApp, "makes payments reconciling", "HTTP")
    
      Person(PersonalBankCustomer, "Personal bank customer", "A customer of the bank that has a personal bank account")
      Rel(PersonalBankCustomer, PaymentProcessingApp, "makes payments checks payments results", "HTTP")
    
      System_Boundary(SystemBoundary, "Bank payment processing system") {
        Container(PaymentReportingApp, "Payment reporting web app", "TypeScript, Angular", "Web application providing payment reports")
        
        %% FE Apps:
        Rel(PaymentReportingApp, PaymentReportingService, "takes payments reports", "HTTP")
          
        Container(PaymentProcessingApp, "Payment processing web app", "TypeScript, Angular", "Web application providing payment processing functionality")
        Rel(PaymentProcessingApp, PaymentProcessingService, "starts payment processing
reads payment statuses", "HTTP")

        %% BE Services:
        Container(PaymentReportingService, "Payment reporting service", "Java, Spring boot", "Service provides reports for analysts.
Reads and agregates data from processing database + external bank database")
        Rel(PaymentReportingService, PaymentProcessingDatabase, "reads payments data", "JDBC")
        Rel(PaymentReportingService, BankDataSystem, "reads payments data", "HTTP")  
              
        Container(PaymentProcessingService, "Payment processing service", "Java, Spring boot", "Service processing payments. Takes care about durable and fast payment processing")
        Rel(PaymentProcessingService, PaymentProcessingDatabase, "saves/reads payment processing details", "JDBC")
        Rel(PaymentProcessingService, PaymentsStatusesTopics, "puts/read payment ids with statuses", "TCP")
        Rel(PaymentProcessingService, FinancialMonitoringSystem, "send payments to check", "HTTP")
        Rel(PaymentProcessingService, BankDataSystem, "saves and retrieves payments
with final statuses", "HTTP")


        %% Storages:      
        ContainerDb(PaymentProcessingDatabase, "Payment processing database", "Oracle Database 12", "Stores payment processing details, errors and statuses")
        ContainerQueue(PaymentsStatusesTopics, "Payments statuses topics", "Apache Kafka", "Provides event messaging for payment processing")
     
      }
      
      System_Boundary(ExternalSystemBoundary, "External systems") {
        System(FinancialMonitoringSystem, "Financial monitoring system", "External financial monitoring system")
        UpdateElementStyle(FinancialMonitoringSystem, $bgColor="grey")
      
        System(BankDataSystem, "Bank data system", "Core legacy bank system. Stores the bank data.")
        UpdateElementStyle(BankDataSystem, $bgColor="grey")
      }

      UpdateLayoutConfig($c4ShapeInRow="2", $c4BoundaryInRow="1")
```

_The service is called by payment processing web application, makes calls to bank data system, financial monitoring system, kafka message broker and its own database._

## Involved flows

### Payment processing sequence:

```mermaid
sequenceDiagram;
    Payment processing web app->>+Payment processing service: send payment
    
    Payment processing service->>+Payment processing database: create new payment
    Payment processing database-)-Payment processing service: created payment
    
    Payment processing service->>Payments statuses topics: add to payment "created" topic

    Payment processing service->>+Bank data system: verify payment by bank
    Bank data system-)-Payment processing service: verification results

    Payment processing service->>+Payment processing database: save processing results
    Payment processing database-)-Payment processing service: updated payment
    
    Payment processing service->>Payments statuses topics: add to payment "verified by bank" topic
    
    Payment processing service->>+Financial monitoring system: verify payment by fin. mon
    Financial monitoring system-)-Payment processing service: verification results

    Payment processing service->>+Payment processing database: save processing results
    Payment processing database-)-Payment processing service: updated payment
    
    Payment processing service->>Payments statuses topics: add to payment "processed" topic

    Payment processing service->>+Bank data system: save to bank system
    Bank data system-)-Payment processing service: saving results

    Payment processing service-)-Payment processing web app: processing results
```
1. User starts payments processing using web application 
2. Web application calls a processing method in payment processing service (PPS)
3. PPS saves the payment data in its database \
  3.1. If some error occur - PPS sends payment id, status and error message to error kafka topic and stops the flow. \
  3.2. If saving was successful - PPS sends payment id to a “created payments” kafka topic and go to “4.” 
4. PPS calls “verify payment” jdbc metod of “bank payment system” \
  4.1. If some error occur - PPS sends payment id, status and error message to error kafka topic and stops the flow. \
  4.2. If verification was successful - PPS sends payment id to a “verified by bank payments” kafka topic and go to “5.” 
5. PPS calls “verify payment” restmetod of “finantial monitoring system” \
  5.1. If some error occur - PPS sends payment id, status and error message to error kafka topic and stops the flow. \
  5.2. If verification was successful - PPS sends payment id to a “verified by financial monitoring payments” kafka topic and go to “6.” \

**TBD:** _to increase the process speed - we can make 4 and 5 in parallel. 6 in that case should wait until payment will be moved to “verified by bank and fin. mon.” status. #After_MVP_

6. PPS saves the payments in bank payment system using “save payment” jdbc method. \
  6.1. If some error occur - PPS sends payment id, status and error message to error kafka topic and stops the flow. \
  6.2. If savingwas successful - PPS stops the flow. \

**TBD:** _implement an “error listener” which will listen “error kafka topic” and based on the error message and payment status will make some actions on them (retry/ error notification/cancel etc.) #After_MVP__

### Payment processing service flow:

```mermaid
flowchart TD;
    S((Start)) --> CP(Create payment) --> EC1{error occur?}
        EC1 -- Yes --> SE(Send error) --> E((Finish))
        EC1 -- No --> SSS1(send successful status) --> VB(verify payment by bank)
            VB --> SPD1(save process details) --> EC2{error occur?}
            EC2 -- Yes --> SE(Send error)
            EC2 -- No --> SSS2(send successful status) --> VF(verify payment by fin. mon)
                VF --> SPD2(save process details) --> EC3{error occur?}
                EC3 -- Yes --> SE(Send error)
                EC3 -- No --> SSS3(send successful status) --> SB(save to bank system)
                    SB --> EC4{error occur?}
                    EC4 -- Yes --> SE(Send error)
                    EC4 -- No --> E
```
_Text description..._