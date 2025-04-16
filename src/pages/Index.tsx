
import React, { useState } from 'react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ChevronRight, Database, FileText, RefreshCw } from "lucide-react";
import SourceConfig from '@/components/SourceConfig';
import TargetConfig from '@/components/TargetConfig';
import ColumnSelector from '@/components/ColumnSelector';
import StatusDisplay from '@/components/StatusDisplay';
import { useToast } from '@/hooks/use-toast';

// Types for our application
export type ConnectionType = 'clickhouse' | 'flatfile';
export type Direction = 'source' | 'target';
export type Status = 'idle' | 'connecting' | 'fetching' | 'ingesting' | 'completed' | 'error';

export interface ClickHouseConfig {
  host: string;
  port: string;
  database: string;
  user: string;
  jwtToken: string;
  table?: string;
}

export interface FlatFileConfig {
  filename: string;
  delimiter: string;
}

export interface Column {
  name: string;
  type: string;
  selected: boolean;
}

const Index = () => {
  const { toast } = useToast();
  const [sourceType, setSourceType] = useState<ConnectionType>('clickhouse');
  const [targetType, setTargetType] = useState<ConnectionType>('flatfile');
  const [sourceConfig, setSourceConfig] = useState<ClickHouseConfig | FlatFileConfig>({
    host: '',
    port: '8123',
    database: '',
    user: '',
    jwtToken: '',
  });
  const [targetConfig, setTargetConfig] = useState<ClickHouseConfig | FlatFileConfig>({
    filename: '',
    delimiter: ',',
  });
  const [columns, setColumns] = useState<Column[]>([]);
  const [status, setStatus] = useState<Status>('idle');
  const [currentStep, setCurrentStep] = useState<number>(1);
  const [recordCount, setRecordCount] = useState<number | null>(null);
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [error, setError] = useState<string | null>(null);

  // Function to handle changes in source configuration
  const handleSourceConfigChange = (config: ClickHouseConfig | FlatFileConfig) => {
    setSourceConfig(config);
  };

  // Function to handle changes in target configuration
  const handleTargetConfigChange = (config: ClickHouseConfig | FlatFileConfig) => {
    setTargetConfig(config);
  };

  // Function to toggle column selection
  const toggleColumnSelection = (index: number) => {
    const updatedColumns = [...columns];
    updatedColumns[index].selected = !updatedColumns[index].selected;
    setColumns(updatedColumns);
  };

  // Function to select/deselect all columns
  const toggleAllColumns = (selected: boolean) => {
    const updatedColumns = columns.map(column => ({
      ...column,
      selected
    }));
    setColumns(updatedColumns);
  };

  // Function to connect to the source and fetch schema
  const connectToSource = async () => {
    // In a real application, this would make an API call
    setStatus('connecting');
    setError(null);
    
    // Simulate API call delay
    await new Promise(resolve => setTimeout(resolve, 1500));
    
    try {
      if (sourceType === 'clickhouse') {
        // Simulate fetching tables from ClickHouse
        setTables(['users', 'orders', 'products', 'transactions']);
        setStatus('idle');
      } else {
        // Simulate fetching columns from flat file
        const mockColumns: Column[] = [
          { name: 'id', type: 'INTEGER', selected: true },
          { name: 'name', type: 'STRING', selected: true },
          { name: 'email', type: 'STRING', selected: true },
          { name: 'created_at', type: 'TIMESTAMP', selected: true },
          { name: 'status', type: 'STRING', selected: true },
          { name: 'amount', type: 'FLOAT', selected: true },
        ];
        setColumns(mockColumns);
        setCurrentStep(3);
        setStatus('idle');
      }
      toast({
        title: "Connection Successful",
        description: sourceType === 'clickhouse' ? "Connected to ClickHouse database" : "Loaded flat file schema",
      });
    } catch (err) {
      setStatus('error');
      setError('Failed to connect to source');
      toast({
        title: "Connection Failed",
        description: "Could not connect to the data source",
        variant: "destructive",
      });
    }
  };

  // Function to load columns from a selected table
  const loadColumns = async () => {
    if (!selectedTable && sourceType === 'clickhouse') {
      toast({
        title: "No Table Selected",
        description: "Please select a table first",
        variant: "destructive",
      });
      return;
    }
    
    setStatus('fetching');
    setError(null);
    
    // Simulate API call delay
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    try {
      // Mock columns for the selected table
      const mockColumns: Column[] = [
        { name: 'id', type: 'INTEGER', selected: true },
        { name: 'user_id', type: 'INTEGER', selected: true },
        { name: 'product_id', type: 'INTEGER', selected: true },
        { name: 'quantity', type: 'INTEGER', selected: true },
        { name: 'price', type: 'FLOAT', selected: true },
        { name: 'created_at', type: 'TIMESTAMP', selected: true },
      ];
      setColumns(mockColumns);
      setStatus('idle');
      setCurrentStep(3);
      toast({
        title: "Columns Loaded",
        description: `Loaded ${mockColumns.length} columns from ${selectedTable}`,
      });
    } catch (err) {
      setStatus('error');
      setError('Failed to load columns');
      toast({
        title: "Failed to Load Columns",
        description: "Could not fetch column information",
        variant: "destructive",
      });
    }
  };

  // Function to start the data ingestion process
  const startIngestion = async () => {
    const selectedColumns = columns.filter(col => col.selected);
    
    if (selectedColumns.length === 0) {
      toast({
        title: "No Columns Selected",
        description: "Please select at least one column for ingestion",
        variant: "destructive",
      });
      return;
    }
    
    setStatus('ingesting');
    setError(null);
    setRecordCount(null);
    
    // Simulate ingestion process
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    try {
      // Mock successful ingestion
      const count = Math.floor(Math.random() * 10000) + 1000;
      setRecordCount(count);
      setStatus('completed');
      toast({
        title: "Ingestion Complete",
        description: `Successfully processed ${count.toLocaleString()} records`,
      });
    } catch (err) {
      setStatus('error');
      setError('Failed to complete data ingestion');
      toast({
        title: "Ingestion Failed",
        description: "An error occurred during data transfer",
        variant: "destructive",
      });
    }
  };

  // Function to reset the process
  const resetProcess = () => {
    setCurrentStep(1);
    setColumns([]);
    setStatus('idle');
    setRecordCount(null);
    setError(null);
    setSelectedTable('');
    setTables([]);
  };

  // Select appropriate next step button based on current step
  const renderNextStepButton = () => {
    if (currentStep === 1) {
      return (
        <Button 
          onClick={connectToSource} 
          disabled={status === 'connecting'}
          className="mt-6"
        >
          {status === 'connecting' ? (
            <>
              <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
              Connecting...
            </>
          ) : (
            <>
              Connect to Source
              <ChevronRight className="ml-2 h-4 w-4" />
            </>
          )}
        </Button>
      );
    } else if (currentStep === 2) {
      return (
        <Button 
          onClick={loadColumns} 
          disabled={status === 'fetching' || !selectedTable}
          className="mt-6"
        >
          {status === 'fetching' ? (
            <>
              <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
              Loading Columns...
            </>
          ) : (
            <>
              Load Columns
              <ChevronRight className="ml-2 h-4 w-4" />
            </>
          )}
        </Button>
      );
    } else if (currentStep === 3) {
      return (
        <Button 
          onClick={startIngestion} 
          disabled={status === 'ingesting' || columns.filter(c => c.selected).length === 0}
          className="mt-6"
        >
          {status === 'ingesting' ? (
            <>
              <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
              Ingesting Data...
            </>
          ) : (
            <>
              Start Ingestion
              <ChevronRight className="ml-2 h-4 w-4" />
            </>
          )}
        </Button>
      );
    }
    
    return null;
  };

  return (
    <div className="container mx-auto p-4 py-8">
      <div className="space-y-6">
        <div className="text-center mb-8">
          <h1 className="text-3xl font-bold text-databridge-800">Data Voyager Bridge</h1>
          <p className="text-muted-foreground mt-2">Transfer data between ClickHouse and Flat Files effortlessly</p>
        </div>
        
        <Card>
          <CardHeader>
            <CardTitle>Configure Data Flow</CardTitle>
            <CardDescription>
              Select source and target data systems
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <h3 className="font-medium mb-2 flex items-center text-databridge-700">
                  <Database className="w-4 h-4 mr-2" />
                  Source
                </h3>
                <Tabs
                  defaultValue={sourceType}
                  onValueChange={(value) => setSourceType(value as ConnectionType)}
                  className="w-full"
                >
                  <TabsList className="w-full mb-4">
                    <TabsTrigger value="clickhouse" className="w-1/2">ClickHouse</TabsTrigger>
                    <TabsTrigger value="flatfile" className="w-1/2">Flat File</TabsTrigger>
                  </TabsList>
                  <TabsContent value="clickhouse" className="mt-0">
                    <SourceConfig 
                      type="clickhouse" 
                      config={sourceConfig as ClickHouseConfig}
                      onConfigChange={handleSourceConfigChange}
                    />
                  </TabsContent>
                  <TabsContent value="flatfile" className="mt-0">
                    <SourceConfig 
                      type="flatfile" 
                      config={sourceConfig as FlatFileConfig}
                      onConfigChange={handleSourceConfigChange}
                    />
                  </TabsContent>
                </Tabs>
              </div>
              
              <div>
                <h3 className="font-medium mb-2 flex items-center text-databridge-700">
                  <FileText className="w-4 h-4 mr-2" />
                  Target
                </h3>
                <Tabs
                  defaultValue={targetType}
                  onValueChange={(value) => setTargetType(value as ConnectionType)}
                  className="w-full"
                >
                  <TabsList className="w-full mb-4">
                    <TabsTrigger value="clickhouse" className="w-1/2">ClickHouse</TabsTrigger>
                    <TabsTrigger value="flatfile" className="w-1/2">Flat File</TabsTrigger>
                  </TabsList>
                  <TabsContent value="clickhouse" className="mt-0">
                    <TargetConfig 
                      type="clickhouse" 
                      config={targetConfig as ClickHouseConfig}
                      onConfigChange={handleTargetConfigChange}
                    />
                  </TabsContent>
                  <TabsContent value="flatfile" className="mt-0">
                    <TargetConfig 
                      type="flatfile" 
                      config={targetConfig as FlatFileConfig}
                      onConfigChange={handleTargetConfigChange}
                    />
                  </TabsContent>
                </Tabs>
              </div>
            </div>
            
            {renderNextStepButton()}
          </CardContent>
        </Card>
        
        {(currentStep >= 2 && sourceType === 'clickhouse' && tables.length > 0) && (
          <Card className="animate-slide-in">
            <CardHeader>
              <CardTitle>Select Table</CardTitle>
              <CardDescription>
                Choose a table from the database
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                {tables.map(table => (
                  <Button
                    key={table}
                    variant={selectedTable === table ? "default" : "outline"}
                    onClick={() => setSelectedTable(table)}
                    className="justify-start"
                  >
                    {table}
                  </Button>
                ))}
              </div>
              {renderNextStepButton()}
            </CardContent>
          </Card>
        )}
        
        {currentStep >= 3 && columns.length > 0 && (
          <Card className="animate-slide-in">
            <CardHeader>
              <CardTitle>Select Columns</CardTitle>
              <CardDescription>
                Choose which columns to include in the data transfer
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ColumnSelector 
                columns={columns} 
                onToggleColumn={toggleColumnSelection}
                onToggleAll={toggleAllColumns}
              />
              {renderNextStepButton()}
            </CardContent>
          </Card>
        )}
        
        {(status === 'ingesting' || status === 'completed' || status === 'error') && (
          <Card className="animate-slide-in">
            <CardHeader>
              <CardTitle>Status & Results</CardTitle>
              <CardDescription>
                Data ingestion progress and results
              </CardDescription>
            </CardHeader>
            <CardContent>
              <StatusDisplay 
                status={status} 
                recordCount={recordCount}
                error={error}
              />
              
              {status === 'completed' && (
                <Button 
                  onClick={resetProcess} 
                  variant="outline" 
                  className="mt-4"
                >
                  Start New Transfer
                </Button>
              )}
              
              {status === 'error' && (
                <Button 
                  onClick={() => startIngestion()}
                  className="mt-4"
                >
                  Retry
                </Button>
              )}
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
};

export default Index;
