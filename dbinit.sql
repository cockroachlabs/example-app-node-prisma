CREATE TABLE "Customer" (
    id UUID PRIMARY KEY
);

CREATE TABLE "Account" (
    id UUID PRIMARY KEY,
    customer_id UUID REFERENCES "Customer"("id") ON DELETE CASCADE,
    balance INT8
);
