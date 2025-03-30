USE Robustor
GO

DROP TABLE IF EXISTS Outbox;

CREATE TABLE Outbox
(
    Id UNIQUEIDENTIFIER NOT NULL,
    Topic NVARCHAR(100) NOT NULL,
    Type NVARCHAR(100) NOT NULL,
    TraceContext NVARCHAR(55) NOT NULL,
    Message NVARCHAR(MAX) NOT NULL,
    CreatedAt datetimeoffset NOT NULL,

    CONSTRAINT "PK_Outbox" PRIMARY KEY ("Id")
);

CREATE INDEX idx_outbox_created_at ON Outbox(CreatedAt)