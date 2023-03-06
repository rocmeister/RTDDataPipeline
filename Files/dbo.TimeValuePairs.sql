USE [TRADEDBRockyTest]
GO

/****** Object:  Table [dbo].[TimeValuePairs] ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[TimeValuePairs](
	[Key] [varchar](20) NOT NULL,
	[Time] [datetime] NOT NULL,
	[Value] [real] NOT NULL,
 CONSTRAINT [PK_TimeValuePairs] PRIMARY KEY CLUSTERED 
(
	[Time] ASC,
	[Key] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

