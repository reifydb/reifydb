import {useState} from 'react'
import {Plus, Database, MoreVertical, Edit, Trash} from 'lucide-react'
import {Button} from '@/components/ui/button'
import {Card, CardContent, CardHeader, CardTitle} from '@/components/ui/card'

interface Schema {
    id: string
    name: string
    fields: number
    records: number
    created: string
    updated: string
}

const mockSchemas: Schema[] = [
    {id: '1', name: 'system', fields: 8, records: 1234, created: '2024-01-01', updated: '2024-01-15'},
]

export function SchemaPage() {
    const [schemas] = useState<Schema[]>(mockSchemas)


    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Database Schema</h1>
                    <p className="text-muted-foreground">Manage your database structure</p>
                </div>
                <Button>
                    <Plus className="mr-2 h-4 w-4"/>
                    New Schema
                </Button>
            </div>


            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                {schemas.map((schema) => (
                    <Card key={schema.id}
                          className="group hover:shadow-xl transition-all duration-200 cursor-pointer hover:-translate-y-1 hover:border-primary/50">
                        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                            <CardTitle className="text-sm font-medium">
                                <div className="flex items-center gap-2">
                                    <Database
                                        className="h-4 w-4 text-primary transition-transform duration-200 group-hover:scale-110"/>
                                    {schema.name}
                                </div>
                            </CardTitle>
                            <Button variant="ghost" size="icon"
                                    className="h-8 w-8 opacity-0 group-hover:opacity-100 transition-opacity">
                                <MoreVertical className="h-4 w-4"/>
                            </Button>
                        </CardHeader>
                        <CardContent>
                            <div className="space-y-2">
                                <div className="flex justify-between text-sm">
                                    <span className="text-muted-foreground">Fields</span>
                                    <span className="font-medium">{schema.fields}</span>
                                </div>
                                <div className="flex justify-between text-sm">
                                    <span className="text-muted-foreground">Records</span>
                                    <span className="font-medium">{schema.records.toLocaleString()}</span>
                                </div>
                                <div className="flex justify-between text-sm">
                                    <span className="text-muted-foreground">Updated</span>
                                    <span className="font-medium">{new Date(schema.updated).toLocaleDateString()}</span>
                                </div>
                            </div>
                            <div className="flex gap-2 mt-4 pt-4 border-t">
                                <Button variant="outline" size="sm"
                                        className="flex-1 group/btn hover:border-primary hover:text-primary">
                                    <Edit className="mr-2 h-3 w-3 transition-transform group-hover/btn:rotate-12"/>
                                    Edit
                                </Button>
                                <Button variant="outline" size="sm"
                                        className="flex-1 group/btn hover:border-destructive hover:text-destructive">
                                    <Trash className="mr-2 h-3 w-3 transition-transform group-hover/btn:scale-110"/>
                                    Delete
                                </Button>
                            </div>
                        </CardContent>
                    </Card>
                ))}
            </div>
        </div>
    )
}