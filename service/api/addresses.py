import logging, sys

from datetime import datetime, timedelta

from flask import abort, jsonify
from webargs.flaskparser import use_args

from marshmallow import Schema, fields

from service.server import app, db
from service.models import AddressSegment
from service.models import Person

log = logging.getLogger("my-logger")


# define Python user-defined exceptions
class Error(Exception):
    """Base class for other exceptions"""
    pass


class InvalidDateError(Error):
    """Raised when the date is not vlaid"""
    pass


class GetAddressQueryArgsSchema(Schema):
    date = fields.Date(required=False, missing=datetime.utcnow().date())


class AddressSchema(Schema):
    class Meta:
        ordered = True

    street_one = fields.Str(required=True, max=128)
    street_two = fields.Str(max=128)
    city = fields.Str(required=True, max=128)
    state = fields.Str(required=True, max=2)
    zip_code = fields.Str(required=True, max=10)

    start_date = fields.Date(required=True)
    end_date = fields.Date(required=False)


@app.route("/api/persons/<uuid:person_id>/address", methods=["GET"])
@use_args(GetAddressQueryArgsSchema(), location="querystring")
def get_address(args, person_id):
    person = Person.query.get(person_id)
    if person is None:
        abort(404, description="person does not exist")
    elif len(person.address_segments) == 0:
        abort(404, description="person does not have an address, please create one")

    address_segment = person.address_segments[-1]
    if args:
        
        arg_date        = args["date"]
        
        # address_segment = map(lambda x: x if (x.start_date == args["date"]), person.address_segments)
        # if len(address_segment) == 1 :
            # address_segment = address_segment[0]
            
            
        for segment in person.address_segments:
            segment_start_date  = segment.start_date
            segment_street_one = segment.street_one
            log.debug(f"NOYO-APP {segment_street_one}- {segment_start_date}, {arg_date}")
            
            if  segment.start_date == args["date"] :
                log.info("NOYO-APP:***************Match*************") 
                address_segment  = segment
                break   
    return jsonify(AddressSchema().dump(address_segment))


@app.route("/api/persons/<uuid:person_id>/address", methods=["PUT"])
@use_args(AddressSchema())
def create_address(payload, person_id):
    person = Person.query.get(person_id)
    if person is None:
        abort(404, description="person does not exist")
    # If there are no AddressSegment records present for the person, we can go
    # ahead and create with no additional logic.
    elif len(person.address_segments) == 0:
        address_segment = AddressSegment(
            street_one=payload.get("street_one"),
            street_two=payload.get("street_two"),
            city=payload.get("city"),
            state=payload.get("state"),
            zip_code=payload.get("zip_code"),
            start_date=payload.get("start_date"),
            person_id=person_id,
        )

        db.session.add(address_segment)
        db.session.commit()
        db.session.refresh(address_segment)
    else:
        # TODO: Implementation
        # If there are one or more existing AddressSegments, create a new AddressSegment
        # that begins on the start_date provided in the API request and continues
        # into the future. If the start_date provided is not greater than most recent
        # address segment start_date, raise an Exception.

        #####################################################################################
        ################# Extention one #####################################################
        most_recent_start_date = person.address_segments[-1].start_date.strftime('%Y-%m-%d') 
        payload_start_date     = payload.get("start_date").strftime('%Y-%m-%d') 
        
        print(f"Recent-{most_recent_start_date} <======> current - {payload_start_date}",file=sys.stdout)
        address_segments = person.address_segments
        s1 = address_segments[-1].street_one
        p_s1 =  payload.get("street_one")
        
        
        if payload_start_date == most_recent_start_date :
            msg = f"Address segment already exists with start_date {payload_start_date}"
            return jsonify({"error": msg}), 422
        elif payload_start_date < most_recent_start_date:
            raise InvalidDateError()    
            log.debug("NOYO-APP","Duplicate Date Key")
        ############################## Extention Three ########################################       
        elif p_s1 == s1  :
            address_segment = address_segments[-1]
            log.debug(f"NOYO-APP:Duplicate Address{p_s1}{payload_start_date}") 
        #######################################################################################  
        else:
           ###################################################################################
           #################################### Extension   Two ##############################
           ################################################################################### 
            address_segment = AddressSegment(
                street_one=payload.get("street_one"),
                street_two=payload.get("street_two"),
                city=payload.get("city"),
                state=payload.get("state"),
                zip_code=payload.get("zip_code"),
                start_date=payload.get("start_date"),
                person_id=person_id
                )

            person.address_segments.append(address_segment)
            log.debug("NOYO-APP:Adding new address to the address segments of the current person")   
            db.session.commit()
            db.session.refresh(person)   
            

    return jsonify(AddressSchema().dump(address_segment))



            
        
        
            
        
